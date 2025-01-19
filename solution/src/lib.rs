use std::{collections::HashMap, time::SystemTime};

use bincode::Error;
use module_system::{Handler, ModuleRef, System, TimerHandle};
use uuid::Uuid;
use std::collections::HashSet;
use tokio::sync::mpsc::UnboundedSender;
use rand::{self, Rng};
use std::cmp::min;

pub use domain::*;

mod domain;

type LeaderMap = HashMap<Uuid, usize>;

#[non_exhaustive]
pub struct Raft {
    // TODO you can add fields to this struct.
    process_type: ProcessType,
    config: ServerConfig,
    persistent_state: PersistentState,
    /// Identifier of a process which is thought to be the leader.
    leader_id: Option<Uuid>,
    sender: Box<dyn RaftSender>,
    storage: Box<dyn StableStorage>,
    election_timer: Option<TimerHandle>,
    self_ref: Option<ModuleRef<Self>>,
    heartbeat_timer: Option<TimerHandle>,
    zero_log: LogEntry,
    commit_index: usize,
    last_applied: usize,
    state_machine: Box<dyn StateMachine>,
    client_requests: HashMap<usize, UnboundedSender<ClientRequestResponse>>,
    
    // leader attributes
    next_index: LeaderMap,
    match_index: LeaderMap,
    heartbeat_response: HashSet<Uuid>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let zero_log = Raft::get_zero_log(first_log_entry_timestamp, config.servers.clone());
        let restored_state = Raft::restore_state_storage(&stable_storage, config.self_id, zero_log.clone()).await;

        let self_ref = system
            .register_module(Self {
                process_type: Default::default(),
                config: config,
                persistent_state: restored_state,
                // Identifier of a process which is thought to be the leader.
                leader_id: None,
                sender: message_sender,
                storage: stable_storage,
                election_timer: None,
                self_ref: None,
                heartbeat_timer: None,
                zero_log: zero_log,
                commit_index: 0,
                last_applied: 0,
                state_machine: state_machine,
                client_requests: HashMap::new(),
                
                // leader attributes
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                heartbeat_response: HashSet::new(),
            })
            .await;
        self_ref.send(Init).await;
        self_ref

        // todo!()
    }

    fn get_zero_log(first_log_entry_timestamp: SystemTime, servers: HashSet<Uuid>) ->  LogEntry {
        let entry = LogEntry{
            content: LogEntryContent::Configuration { servers: servers },
            term: 0,
            timestamp: first_log_entry_timestamp,
        };

        entry
    }

    async fn broadcast(&self, msg: RaftMessage) { 

        for target in &self.config.servers {
            if *target != self.config.self_id {
                self.sender.send(&target, msg.clone()).await;
            }
        }
    }

    fn get_self_header(&self) -> RaftMessageHeader {
        let header = RaftMessageHeader{
            source: self.config.self_id,
            term: self.persistent_state.current_term,
        };

        header
    }

    fn get_last_log_term_and_idx(&self) -> (u64, usize)  {
        return (self.get_last_log_term(), self.get_last_log_idx());
    }

    fn is_other_log_at_least_as_up_to_date_as_self(&self, last_log_index: usize, last_log_term: u64) -> bool {
        let (self_term, self_idx) = self.get_last_log_term_and_idx();                
                if last_log_term > self_term {
                    // our term is older
                    return true;
                }
                else { 
                    return (self_term == last_log_term) && (self_idx <= last_log_index);
                }
    } 

    async fn send_request_response(&self, source: Uuid, vote: bool) {
        let self_header = self.get_self_header();
            let response = RaftMessage{
                header: self_header,
                content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs { vote_granted: vote }),
            };

            self.sender.send(&source, response).await;
    }

    async fn update_storage(&mut self) {
        let serialized_storage = bincode::serialize(&self.persistent_state);
        match serialized_storage {
            Err(_) => {
                panic!("Intended panic: erialize storage error");
            },
            Ok(res) => {
                let storage_res = self.storage.put(&self.config.self_id.to_string(), &res).await;
                match storage_res {
                    Err(msg) => {
                        panic!("{}", msg);
                    },
                    Ok(_) => {},
                }
            }
        }
    }

    async fn restore_state_storage(storage: &Box<dyn StableStorage>, self_id: Uuid, zero_log: LogEntry,) -> PersistentState {
        let restore_res = storage.get(&self_id.to_string()).await;
        match restore_res {
            None => {
                let logs = vec![zero_log];
                let state = PersistentState{
                    current_term: 0,
                    voted_for: None,
                    log: logs,
                };

                return state;
            },

            Some(restored) => {
                let deserialize_res: Result<PersistentState, Error> = bincode::deserialize(&restored);
                match deserialize_res {
                    Err(msg) => {
                        panic!("Intended panic: bincode desrialization error in persistent state retrieval: {}", msg);
                    },
                    Ok(state) =>
                        return state,
                }
            }
        }

    }

    async fn update_candidate(&mut self, candidate_id: Uuid) {
        self.persistent_state.voted_for = Some(candidate_id);
        self.update_storage().await;
    }

    async fn update_term(&mut self, new_term: u64) {
        self.persistent_state.current_term = new_term;
        self.update_storage().await;    
    }

    async fn convert_to_follower(&mut self, current_term: u64, leader: Option<Uuid>) {
        self.update_term(current_term).await;

        self.persistent_state.voted_for = None;
        self.update_storage().await;

        self.leader_id = leader;
        self.process_type = ProcessType::Follower;

        self.reset_election_timer().await;

        if let Some(handle) = self.heartbeat_timer.take() {
            handle.stop().await;
        }
    }
    
    async fn reset_election_timer(&mut self) {
        let interval= rand::thread_rng().gen_range(self.config.election_timeout_range.clone());

        if let Some(handle) = self.election_timer.take() {
            handle.stop().await;
        }
        self.election_timer = Some(
            self.self_ref
                .as_ref()
                .unwrap()
                .request_tick(ElectionTimeout, interval)
                .await,
        );
    }

    async fn handle_request_vote(&mut self, request_vote: RequestVoteArgs, request_header: RaftMessageHeader) {
        let RequestVoteArgs { last_log_index, last_log_term } = request_vote;
        let RaftMessageHeader { source, term } = request_header;

        // if our term is newer - reject the message
        // leader sets himself as a leader
        // if we are connected to the leader - reject
        if self.persistent_state.current_term > term { 
            self.send_request_response(source, false).await;
        }
        else if self.leader_id.is_some() {
            // ignore
        }
        else {

            if self.persistent_state.current_term < term {
                // convert to a follower if a term is newer, grant a vote
                self.convert_to_follower(term, None).await;
            }

            if self.is_other_log_at_least_as_up_to_date_as_self(last_log_index, last_log_term) {
                    // we have ruled out self_term > term and self_term < term,
                    // there is only self_term == term left
                    match self.persistent_state.voted_for {
                        None => {
                            self.update_candidate(source).await;
                            self.send_request_response(source, true).await;
                        },
                        Some(_id) => {
                            // we have already voted
                            self.send_request_response(source, false).await;
                        }
                }
            }
            else {
                self.send_request_response(source, false).await;
            }
        }
    }

    fn is_candidate(&self) -> bool {
        if let Some(candidate_id) = self.persistent_state.voted_for {
            return candidate_id == self.config.self_id;
        }

        false
    }


    fn initialize_leader_hashmaps(&self, initial_value: usize) -> LeaderMap {
        let hashmap: LeaderMap = self.config.servers.clone().into_iter().map(|x| (x, initial_value)).collect();

        hashmap
    }

    // Configuration entry is one lement, but it's index is 0,
    // therefore we have to decrement the value 
    fn get_last_log_idx(&self) -> usize {
        self.persistent_state.log.len().saturating_sub(1)
    }

    // fn get_last_log_entry(&self) -> &LogEntry {
    //     let last_log = self.persistent_state.log.last();
    //     match last_log {
    //         None => &self.zero_log,
    //         Some(log) => log,
    //     }
    // }

    fn get_last_log_term(&self) -> u64 {
        let last_log = self.persistent_state.log.last();
        match last_log {
            None => self.zero_log.term,
            Some(log) => log.term,
        }
    }

    fn get_empty_append_entry(&self, follower_id: Uuid) -> RaftMessage{
        return self.get_append_entry(Vec::new(), follower_id);
    }

    fn is_next_idx_next_after_match_idx(&self, follower_id: Uuid) -> bool {
        let match_res = self.match_index.get(&follower_id);
        if let Some(match_idx) = match_res {
            let next_res = self.next_index.get(&follower_id);
            if let Some(next_idx) = next_res {
                if *next_idx == (*match_idx + 1) {
                    return true;
                }
            }
        }

        false
    }

    async fn broadcast_heartbeat(&mut self) {
        let servers = self.config.servers.clone();
        for follower_id in servers { 
            if follower_id != self.config.self_id {
                if self.is_next_idx_next_after_match_idx(follower_id) {
                    self.send_up_to_batch_size(follower_id).await; 
                }
                else {
                    let empty_entry = self.get_empty_append_entry(follower_id);
                    self.sender.send(&follower_id, empty_entry).await;
                }
            }
        }
    }

    fn get_log_entry(&self, content: LogEntryContent) -> LogEntry {
        LogEntry{
            content: content,
            term: self.persistent_state.current_term,
            timestamp: SystemTime::now(),
        }
    }

    async fn push_to_log(&mut self, log: LogEntry) {
        self.persistent_state.log.push(log);
        self.update_storage().await;

        if self.process_type == ProcessType::Leader {
            self.match_index.insert(self.config.self_id, self.get_last_log_idx());
            self.next_index.insert(self.config.self_id, self.get_last_log_idx() + 1);
        }
    }

    async fn push_nop_to_log(&mut self) {
        let nop_entry = self.get_log_entry(LogEntryContent::NoOp);
        self.push_to_log(nop_entry).await;
    }

    async fn become_a_leader(&mut self) {
        self.leader_id = Some(self.config.self_id);
        self.process_type = ProcessType::Leader;

        self.heartbeat_response = HashSet::new();
        self.next_index = self.initialize_leader_hashmaps(self.get_last_log_idx() + 1); // +1 will be nop
        self.match_index = self.initialize_leader_hashmaps(0);
        self.push_nop_to_log().await;

        // self.broadcast_nop().await;
        self.broadcast_heartbeat().await;

        if let Some(handle) = self.heartbeat_timer.take() {
            handle.stop().await;
        }

        self.heartbeat_timer = Some(
            self.self_ref
                .as_ref()
                .unwrap()
                .request_tick(HeartbeatTick, self.config.heartbeat_timeout)
                .await,
        );
    }

    async fn handle_request_response(&mut self, request_response: RequestVoteResponseArgs, header: RaftMessageHeader) {
        if let ProcessType::Candidate { votes_received } = &mut self.process_type {

            if let RequestVoteResponseArgs { vote_granted: true } = request_response {
                votes_received.insert(header.source);
            }

            if votes_received.len() > (self.config.servers.len() / 2) {
                self.become_a_leader().await;
            }
        }
    }

    fn get_append_entry_response(&self, success: bool, last_verified_log_index: usize) -> RaftMessage {
        let header = self.get_self_header();
        let args = AppendEntriesResponseArgs{
            success: success,
            last_verified_log_index: last_verified_log_index,
        };
        let content = RaftMessageContent::AppendEntriesResponse(args);

        return RaftMessage{header: header, content: content};

    }

    fn get_last_verified_log_index(&self, append_entries: &AppendEntriesArgs) -> usize {
        return append_entries.entries.len() + append_entries.prev_log_index;
    }

    async fn send_append_entry_response(&mut self, success: bool, last_verified_log_index: usize, source: Uuid) {
        let response = self.get_append_entry_response(success, last_verified_log_index);
        
        self.sender.send(&source, response).await;
    }
    
    fn are_logs_matching(&self, prev_log_index: usize, prev_log_term: u64) -> bool {
        if self.get_last_log_idx() < prev_log_index {
            return false;
            // we have too few logs
        }
        else {
            let queried_log = &self.persistent_state.log[prev_log_index];
            let queried_term = queried_log.term;

            return queried_term == prev_log_term;
        }
    }

    async fn clear_not_matching_logs(&mut self, last_prev_log_index: usize) {
        if last_prev_log_index < self.get_last_log_idx() {
            let next_not_matching = last_prev_log_index + 1;
            self.persistent_state.log.drain(next_not_matching..);
            self.update_storage().await;
        }
    }

    async fn append_entries(&mut self, entries: &mut Vec<LogEntry>) {
        self.persistent_state.log.append(entries);
        self.update_storage().await;
    }

    async fn apply_log_to_state_machine(&mut self, log: LogEntry) {
        let LogEntry { content, term: _, timestamp: _ } = log;
        if let LogEntryContent::Command { data, client_id: _, sequence_num: _, lowest_sequence_num_without_response: _ } = content {
            self.state_machine.apply(&data).await;
        }
    }

    async fn commit_entries_according_to_commit_idx(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let log_to_be_committed = self.persistent_state.log[self.last_applied].clone();
            self.apply_log_to_state_machine(log_to_be_committed).await;
        }
    }

    // Assuming that the function is called after updating log entries
    fn update_commmit_index(&mut self, leader_commit: usize) {
        if leader_commit > self.commit_index {
            self.commit_index = min(leader_commit, self.get_last_log_idx());
        }
    }

    async fn handle_append_entries(&mut self, append_entries: AppendEntriesArgs, header: RaftMessageHeader) {
        let last_verified_log_index = self.get_last_verified_log_index(&append_entries);


        let AppendEntriesArgs { prev_log_index, prev_log_term, mut entries, leader_commit } = append_entries;


        let RaftMessageHeader{source, term} = header;

        // we got a message from an older term
        if self.persistent_state.current_term > term {
            self.send_append_entry_response(false, last_verified_log_index, source).await;
        }
        else {
            // we have rejected the message with a smaller term
            // now the term is at least as high as ours
            if self.persistent_state.current_term < term || self.is_candidate() || self.leader_id.is_none() {
                self.convert_to_follower(term, Some(source)).await;
            }
            else {
                if let Some(leader_id) = self.leader_id {
                    if leader_id == source {
                        self.reset_election_timer().await;
                    }
                }
                // reset is issued additionally during conversion
            }


            // process the request
            if !self.are_logs_matching(prev_log_index, prev_log_term) {
                self.send_append_entry_response(false, last_verified_log_index, source).await;
            }
            else {
                /*
                Iteratively, we have found the first matching log
                since the leader decremented the last matching index
                Therefore, if we have found the first matching log,
                we can delete entires following the last matching log
                 */
                self.clear_not_matching_logs(prev_log_index).await;
                self.append_entries(&mut entries).await;
                self.update_commmit_index(leader_commit);
                self.commit_entries_according_to_commit_idx().await;

                self.send_append_entry_response(true, last_verified_log_index, source).await;
            }
        }
    }

    fn update_match_idx(&mut self, follower_id: Uuid, last_verified_idx: usize) {
        self.match_index.insert(follower_id, last_verified_idx);
    }


    fn get_prev_idx_term(&self, follower_id: Uuid) -> (usize, u64) {
        let next_idx_res = self.next_index.get(&follower_id);

        match next_idx_res {
            None => return (0, 0),
            Some(next_idx) => {
                // next_log is the log we are going to send
                // -1 is the log before the logs we are going to send
                let prev_idx = (*next_idx).saturating_sub(1);
                let prev_log = &self.persistent_state.log[prev_idx];
                return (prev_idx, prev_log.term);
            }
        }
    }

    fn get_append_entry(&self, entries: Vec<LogEntry>, follower_id: Uuid) -> RaftMessage{
        let header = self.get_self_header();
        let (prev_idx, prev_term) = self.get_prev_idx_term(follower_id);
        let args = AppendEntriesArgs{
            prev_log_index: prev_idx,
            prev_log_term: prev_term,
            entries: entries,
            leader_commit: self.commit_index,
        };
        let content = RaftMessageContent::AppendEntries(args);

        return RaftMessage { header: header, content: content };
    }

    fn update_next_idx_after_success(&mut self, follower_id: Uuid, last_verified_idx: usize) {
        let next_to_insert = last_verified_idx + 1;
        // the value is updated
        self.next_index.insert(follower_id, next_to_insert);
    }

    fn update_decrement_next_idx(&mut self, follower_id: Uuid, last_verified_idx: usize) {
        let next_to_insert = last_verified_idx.saturating_sub(1);
        let next_idx = self.next_index.get_mut(&follower_id);

        match next_idx {
            None => {
                // this should not happen
                self.next_index.insert(follower_id, next_to_insert);
            },
            Some(idx) => {
                *idx = idx.saturating_sub(1);
            }
        }
    }

    async fn send_up_to_batch_size(&mut self, follower_id: Uuid) {
        let match_res = self.match_index.get(&follower_id);

        if let Some(match_idx) = match_res {
            let num_logs_to_send = self.get_last_log_idx().saturating_sub(*match_idx);
            let logs_per_batch = min(num_logs_to_send, self.config.append_entries_batch_size);
            let end_range_first_idx_not_to_be_sent = logs_per_batch + match_idx + 1;
            let start_range_first_idx_to_be_sent = min(match_idx + 1, end_range_first_idx_not_to_be_sent);
            /*
            match_idx is already included
            match_idx = 1
            last_idx = 3
            0 1 || 2 3
            3 - 1 = 2 elements to be sent

            match idx + 1 = 2 /// from the 2nd element, the first element to be sent
            last idx in  range is excluded

            [start..start] gives empty slice if up-to-date
             */
            let entries_to_append = &self.persistent_state.log[start_range_first_idx_to_be_sent..end_range_first_idx_not_to_be_sent];
            let append_entry = self.get_append_entry(entries_to_append.to_vec(), follower_id);


            self.sender.send(&follower_id, append_entry).await;
        }
    }


    // After the crash - the sender might be missing
    // we remove the sender for optimisation
    fn get_sender_send_command(&mut self, response: ClientRequestResponse) {
        let sender_res= self.client_requests.remove(&self.last_applied);

        if let Some(sender) = sender_res {
            sender.send(response).unwrap();
        }
    }

    fn send_register_client_mock_response(&mut self) {
        let content = RegisterClientResponseContent::ClientRegistered { client_id: Uuid::from_u128(self.last_applied as u128) };

        let args = RegisterClientResponseArgs{
            content: content,
        };

        let response = ClientRequestResponse::RegisterClientResponse(args);
        self.get_sender_send_command(response);
    }

    async fn send_command_response_to_client_apply_to_state(&mut self) {
        let log = &self.persistent_state.log[self.last_applied];
        let LogEntry { content, term: _, timestamp: _ } = log.clone();

        if let LogEntryContent::Command { data, client_id, sequence_num, lowest_sequence_num_without_response: _ } = &content {
            let applied_output = self.state_machine.apply(data).await;
            let response_content = CommandResponseContent::CommandApplied { output: applied_output };
            let args = CommandResponseArgs{
                client_id: *client_id,
                sequence_num: *sequence_num,
                content: response_content,
            };

            let response = ClientRequestResponse::CommandResponse(args);
            self.get_sender_send_command(response);
        }

        if let LogEntryContent::RegisterClient = &content {

            self.send_register_client_mock_response();
        }

    }

    async fn commit_entries_send_to_client(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            self.send_command_response_to_client_apply_to_state().await;
        }
    }

    async fn commit_and_send_current_entry_and_maybe_previous_if_majority_agrees(&mut self, last_verified_idx: usize) {
        if last_verified_idx > self.commit_index {
            // with new success - the majority might agree now
            let agreed = self.match_index.values().filter(|&&x| x >= last_verified_idx).count();
            if agreed > (self.config.servers.len() / 2) {
                // the majority agrees
                if self.persistent_state.current_term == self.persistent_state.log[last_verified_idx].term {
                    // if the next entry to commit is from current term
                    // commmit this entry and all previous
                    self.commit_index = last_verified_idx;
                    self.commit_entries_send_to_client().await;
                }
            }
        }
    }
    /*
    prev_log_index = 2
    entries.len() = 3

    0 1 2 || x x x
    0 1 2 || 3 4 5
    2 + 3 = 5
    last_verified_log_index is the index of the last entry which is known to reside in the log

    what if we decremented and send empty entries?
    prev_log_index = 2
    entries.len() = 0

    0 1 2 ||
    still the last available index
    
     */
    async fn handle_append_entries_response(&mut self, append_entries_response: AppendEntriesResponseArgs, header: RaftMessageHeader) {
        let AppendEntriesResponseArgs { success, last_verified_log_index } = append_entries_response;

        let RaftMessageHeader { source, term } = header;

        // check term for update
        if self.persistent_state.current_term < term {
            self.convert_to_follower(term, None).await;
        }
        else {
            self.heartbeat_response.insert(source);

            if success {
                // last_verified_log_index = entries.len() + prev_index
                // the full length of the follower log
                // the index of the last common entry which is known to reside in the log
                
                // update matchIdx to last verified
                // check the entries length
                // send a batch
                // don't send multiple batches - network errors possible, reordering possible
                // last_verified_log - last idx present in both server and a follower
                // last_log_idx - idx of the last log in the server
                // send messages only if the follower is behind the server
                self.update_match_idx(source, last_verified_log_index);
                self.update_next_idx_after_success(source, last_verified_log_index);

                // nextIdx should be matchIdx + 1
                // we want to send messages if there are any left to be sent
                if last_verified_log_index != self.get_last_log_idx() {
                    // all logs are replicated on the given server
                    // for heartbeat - it would eventually send empty messages
                    self.send_up_to_batch_size(source).await;
                }

                // when to update match and next indices?
                /*
                success - the entries have been appended, therefore we can update the match index
                */

                // SUCCESS - check if possible to commit
                // send to a client
                self.commit_and_send_current_entry_and_maybe_previous_if_majority_agrees(last_verified_log_index).await;
                // }
            }
            else {
                    // we are still a leader
                    self.update_decrement_next_idx(source, last_verified_log_index);
                    let empty_entry = self.get_empty_append_entry(source);
                    self.sender.send(&source, empty_entry).await;
            }
        }
    }

    // client commands
    async fn add_client_command_to_log(&mut self, command: ClientRequestContent) {
        let mut log_content = LogEntryContent::RegisterClient;

        if let ClientRequestContent::Command { command, client_id, sequence_num, lowest_sequence_num_without_response } = &command {
            log_content = LogEntryContent::Command { data: command.to_vec(), client_id: *client_id, sequence_num: *sequence_num, lowest_sequence_num_without_response: *lowest_sequence_num_without_response };
        }

        let log_entry = self.get_log_entry(log_content);
        self.push_to_log(log_entry).await;
    }

    async fn send_already_added_new_log_to_ready_followers(&mut self) {
        // let last_log = self.get_last_log_entry();
        let servers = self.config.servers.clone();

        for follower_id in servers { //&self.config.servers {
            if follower_id != self.config.self_id {
                // let next_idx_res = self.next_index.get(follower_id);
                // if let Some(next_idx) = next_idx_res {
                //     if *next_idx == self.get_last_log_idx() {
                    if self.is_next_idx_next_after_match_idx(follower_id) {
                        // the follower is up to date
                        self.send_up_to_batch_size(follower_id).await;
                        // let wrapped_entry = vec![last_log.clone()];
                        // let entry_to_send = self.get_append_entry(wrapped_entry, *follower_id);
                        // self.sender.send(follower_id, entry_to_send).await;
                    }
                    else {
                        /*
                        When a leader has log entries to send to a follower, it should send AppendEntries immediately (rather than send AppendEntries only on heartbeat timeouts).

                        I understand that we should always send AppendEntry when there is a new log, even if nextIdx != matchIdx + 1
                        */
                        let empty_entry = self.get_empty_append_entry(follower_id);
                        self.sender.send(&follower_id, empty_entry).await;
                    }
                
            }
        }
    }

    fn decline_client_command_send_leader_id(&self, content: ClientRequestContent, reply_to: UnboundedSender<ClientRequestResponse>) {
            if let ClientRequestContent::Command { command: _, client_id, sequence_num, lowest_sequence_num_without_response: _ } = &content {
                let response_content = CommandResponseContent::NotLeader { leader_hint: self.leader_id };
                let args = CommandResponseArgs{
                    client_id: *client_id, sequence_num: *sequence_num, content: response_content};
                let response = ClientRequestResponse::CommandResponse(args);
                reply_to.send(response).unwrap();
            }

            if let ClientRequestContent::RegisterClient = &content {
                let response_content = RegisterClientResponseContent::NotLeader { leader_hint: self.leader_id };
                let args = RegisterClientResponseArgs{
                    content: response_content,
                };
                let response = ClientRequestResponse::RegisterClientResponse(args);
                reply_to.send(response).unwrap();
            }
        }



    async fn handle_client_command_request(&mut self, command: ClientRequestContent, reply_to: UnboundedSender<ClientRequestResponse>) {
        if self.process_type != ProcessType::Leader {
            // inform that you are not a leader, send leader id
            self.decline_client_command_send_leader_id(command, reply_to);
        }
        else {
            self.add_client_command_to_log(command).await;

            let command_idx = self.get_last_log_idx();
            self.client_requests.insert(command_idx, reply_to); 
            self.send_already_added_new_log_to_ready_followers().await;
        }
    }

    async fn broadcast_request_vote(&mut self) {
        let (last_term, last_idx) = self.get_last_log_term_and_idx();
        let args = RequestVoteArgs{
            last_log_index: last_idx,
            last_log_term: last_term,
        };
        let content = RaftMessageContent::RequestVote(args);
        
        let header = self.get_self_header();
        let msg = RaftMessage { header: header, content: content,};

        self.broadcast(msg).await;
    }

    async fn convert_to_a_candidate_start_election(&mut self) {
        let new_term = self.persistent_state.current_term + 1;
        self.update_term(new_term).await;

        let mut votes_granted: HashSet<Uuid> = HashSet::new();
        votes_granted.insert(self.config.self_id);
        self.process_type = ProcessType::Candidate { votes_received: votes_granted };

        self.update_candidate(self.config.self_id).await;
        self.leader_id = None;
        self.reset_election_timer().await;

        self.broadcast_request_vote().await;
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        let RaftMessage { header, content } = msg;

        match content {
            RaftMessageContent::AppendEntries(append_entries) => {
                self.handle_append_entries(append_entries, header).await;
            },
            RaftMessageContent::AppendEntriesResponse(append_entry_response) => {
                self.handle_append_entries_response(append_entry_response, header).await;
            },
            RaftMessageContent::RequestVote(request_vote_args) => {
                self.handle_request_vote(request_vote_args, header).await;
            },
            RaftMessageContent::RequestVoteResponse(request_vote_response) => {
                self.handle_request_response(request_vote_response, header).await;
            },
            RaftMessageContent::InstallSnapshot(_) => {
                unimplemented!("Snapshots omitted");
            },
            RaftMessageContent::InstallSnapshotResponse(_) => {
                unimplemented!("Snapshots omitted")
            },
        }
        // todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
            let ClientRequest { reply_to, content } = msg;

        
                match &content {
                    ClientRequestContent::Command { command: _, client_id: _, sequence_num: _, lowest_sequence_num_without_response: _ } => {
                        self.handle_client_command_request(content, reply_to).await;
                    },
                    ClientRequestContent::Snapshot => {
                        unimplemented!("Snapshots omitted");
                    },
                    ClientRequestContent::AddServer { new_server: _ } => {
                        unimplemented!("Cluster membership changes omitted");
                    },
                    ClientRequestContent::RemoveServer { old_server: _ } => {
                        unimplemented!("Cluster membership changes omitted");
                    },
                    ClientRequestContent::RegisterClient => {
                        self.handle_client_command_request(content, reply_to).await;
                    },
                }
            
        }
        // todo!()
}

// TODO you can implement handlers of messages of other types for the Raft struct.
/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _: ElectionTimeout) {
        match &mut self.process_type  {
            ProcessType::Follower => {
                self.convert_to_a_candidate_start_election().await;
            },
            ProcessType::Candidate { votes_received } => {
                if votes_received.len() > (self.config.servers.len() / 2) {
                    self.become_a_leader().await;
                }
                else {
                    // start new election
                    self.convert_to_a_candidate_start_election().await;
                }
            },
            ProcessType::Leader => {
                if self.heartbeat_response.len() < (self.config.servers.len() / 2) {
                    self.convert_to_follower(self.persistent_state.current_term, None).await;
                }
                else {
                    self.heartbeat_response.clear();
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatTick> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _: HeartbeatTick) {
        if self.process_type == ProcessType::Leader {
            self.broadcast_heartbeat().await;
        }
        else if let Some(handle) = self.heartbeat_timer.take() {
            // the leader is a leader till the end of its tenure, therefore the leadership change is not expected; however, safety never hurt anybody
            handle.stop().await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Init) {
        self.self_ref = Some(self_ref.clone());
        self.reset_election_timer().await;
    }
}