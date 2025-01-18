use std::{collections::HashMap, time::SystemTime};

use module_system::{Handler, ModuleRef, System, TimerHandle};
use uuid::{timestamp::context, Uuid};
use std::collections::HashSet;
use std::future::Future;
use tokio::time::Duration;
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
    // granted_votes: HashSet<Uuid>,
    // sending_set: HashSet<Uuid>,
    election_timer: Option<TimerHandle>,
    self_ref: Option<ModuleRef<Self>>,
    heartbeat_timer: Option<TimerHandle>,
    zero_log: LogEntry,
    commit_index: usize,
    last_applied: usize,
    state_machine: Box<dyn StateMachine>,
    
    // leader attributes
    next_index: LeaderMap,
    match_index: LeaderMap,
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
        // recover

        todo!()
    }

    async fn broadcast(&self, msg: RaftMessage) { 
        // TODO change to join_all?
        // let futures: Vec<Future> = Vec::new();
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

    fn get_last_log_and_idx(&self) -> (u64, usize)  {
        // return (self.persistent_state.log.last(), self.persistent_state.log.len());
        return (self.get_last_log_term(), self.get_last_log_idx());
    }

    fn is_other_log_at_least_as_up_to_date_as_self(&self, last_log_index: usize, last_log_term: u64) -> bool {
        // let self_last_log_idx
        let (self_term, self_idx) = self.get_last_log_and_idx();

        // match self_log {
        //     // idx zero for empty log
        //     None =>  {
        //     // the sent log has either no elements (as our log),
        //     // thus it is as up-to-date as ours
        //     // or can have any entry, which is more up-to-date as ours
        //         return true;
        //     },

        //     Some(log) => {
                // let self_term = log.term;
                
                if last_log_term > self_term {
                    // our term is older
                    return true;
                }
                else { 
                    return (self_term == last_log_term) && (self_idx <= last_log_index);
                }
        //     }
        // }
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
                panic!("Serialize storage error");
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

    async fn update_candidate(&mut self, candidate_id: Uuid) {
        self.persistent_state.voted_for = Some(candidate_id);
        self.update_storage().await;
    }

    async fn update_term(&mut self, new_term: u64) {
        self.persistent_state.current_term = new_term;
        self.update_storage().await;    
    }

    async fn update_term_if_newer(&mut self, new_term: u64) {
        if self.persistent_state.current_term < new_term {
            self.update_term(new_term).await;
        }
    }

    async fn convert_to_follower(&mut self, current_term: u64, leader: Option<Uuid>) {
        self.update_term(current_term).await;

        self.persistent_state.voted_for = None;
        self.update_storage().await;

        self.leader_id = leader;
        self.process_type = ProcessType::Follower;
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
        if self.persistent_state.current_term > term || self.leader_id.is_some() {
            self.send_request_response(source, false).await;
        }
        else {
            // if self.persistent_state.current_term < term {
            //     // convert to a follower

            // }
            // if log is at least as up-to-date as mine - grant vote, update your vote
            // match self.persistent_state.voted_for {
            //     None => {
            if self.is_other_log_at_least_as_up_to_date_as_self(last_log_index, last_log_term) {
                // TODO check
                if self.persistent_state.current_term < term {
                    // convert to a follower if a term is newer, grant a vote
                    self.convert_to_follower(term, None).await;
                    self.update_candidate(source).await;
                    self.send_request_response(source, true).await;
                }
                else {
                    // we have ruled out self_term > term and self_term < term,
                    // there is only self_term == term left
                    match self.persistent_state.voted_for {
                        None => {
                            self.update_candidate(source).await;
                            self.send_request_response(source, true).await;
                        },
                        Some(_) => {
                            // we have already voted
                            self.send_request_response(source, false).await;
                        }
                    }
                }
            }
            else {
                self.send_request_response(source, false).await;
            }
                // }, 
                // Some(_candidate_id) => {
                //     // if self.is_other_log_at_least_as_up_to_date_as_self(last_log_index, last_log_term) {

                //     // }
            
                // },
        }
    }

    fn is_candidate(&self) -> bool {
        if let Some(candidate_id) = self.persistent_state.voted_for {
            return candidate_id == self.config.self_id;
        }

        false
    }

    fn is_leader(&self) -> bool {
        if let Some(leader_id) = self.leader_id {
            return leader_id == self.config.self_id;
        }

        false
    }

    fn initialize_leader_hashmaps(&self, initial_value: usize) -> LeaderMap {
        let hashmap: LeaderMap = self.config.servers.clone().into_iter().map(|x| (x, initial_value)).collect();

        hashmap
    }

    // COnfiguration entry is one lement, but it's index is 0,
    // therefore we have to decrement the value 
    fn get_last_log_idx(&self) -> usize {
        self.persistent_state.log.len().saturating_sub(1)
    }

    fn get_last_log_entry(&self) -> &LogEntry {
        let last_log = self.persistent_state.log.last();
        match last_log {
            None => &self.zero_log,
            Some(log) => log,
        }
    }

    fn get_last_log_term(&self) -> u64 {
        let last_log = self.persistent_state.log.last();
        match last_log {
            None => self.zero_log.term,
            Some(log) => log.term,
        }
    }

    fn get_empty_append_entry(&self) -> RaftMessage{
        let header = self.get_self_header();
        let args = AppendEntriesArgs{
            prev_log_index: self.get_last_log_idx(),
            prev_log_term: self.get_last_log_term(),
            entries: Vec::new(),
            leader_commit: self.commit_index,
        };
        let content = RaftMessageContent::AppendEntries(args);
     
        return RaftMessage{
            header: header,
            content: content,
        };
    }

    async fn broadcast_heartbeat(&self) {
        let hearbeat = self.get_empty_append_entry();
        self.broadcast(hearbeat).await;
    }

    fn get_log_entry(&self, content: LogEntryContent) -> LogEntry {
        LogEntry{
            content: content,
            term: self.persistent_state.current_term,
            timestamp: SystemTime::now(),
        }
        // unimplemented!()
    }
    async fn push_nop_to_log(&mut self) {
        let nop_entry = self.get_log_entry(LogEntryContent::NoOp);
        self.persistent_state.log.push(nop_entry);
        self.update_storage().await;
    }

    async fn become_a_leader(&mut self) {
        // In LA1, the first tick should be sent after the interval elapses
        self.broadcast_heartbeat();
        self.heartbeat_timer = Some(
            self.self_ref
                .as_ref()
                .unwrap()
                .request_tick(HeartbeatTick, self.config.heartbeat_timeout)
                .await,
        );

        self.leader_id = Some(self.config.self_id);
        self.process_type = ProcessType::Leader;

        self.push_nop_to_log().await;
        self.next_index = self.initialize_leader_hashmaps(self.get_last_log_idx()); // without +1, since nextIdx should be initialized with nop idx
        self.match_index = self.initialize_leader_hashmaps(0);
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

        // unimplemented!()
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
        // unimplemented!()
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
        let serialized_res = bincode::serialize(&log);
        match serialized_res {
            Err(msg) => {
                panic!("Panic induced by the author; bincode error: {}", msg);
            }
            Ok(serialized_log) => {
                self.state_machine.apply(&serialized_log).await;
            }
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
            // we have trejected the message with a smaller term
            // now the term is at least as high as ours
            if self.persistent_state.current_term < term || self.is_candidate() {
                self.convert_to_follower(term, Some(source)).await;
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
        let follower_val = self.match_index.get_mut(&follower_id);
        match follower_val {
            None => {
                self.match_index.insert(follower_id, last_verified_idx);
            },
            Some(val) => {
                *val = last_verified_idx;
            }
        }
    }

    fn get_last_matching_idx_data(&self, follower_id: Uuid) -> (usize, u64) {
        let follower_data = self.match_index.get(&follower_id);
        match follower_data {
            None => {return (0, 0);},
            Some(log_idx) => {
                let term = self.persistent_state.log[*log_idx].term;

                return (*log_idx, term);
            }
        }

        unimplemented!()
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
                return (*next_idx, prev_log.term);
            }
        }
        // unimplemented!()
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

    fn update_next_idx(&mut self, follower_id: Uuid, last_verified_idx: usize) {

    }

    async fn send_up_to_batch_size(&mut self, follower_id: Uuid) {
        let match_res = self.match_index.get(&follower_id);

        if let Some(match_idx) = match_res {
            let num_logs_to_send = self.get_last_log_idx().saturating_sub(*match_idx);
            let logs_per_batch = min(num_logs_to_send, self.config.append_entries_batch_size);
            let end_range_first_idx_not_to_be_sent = logs_per_batch + match_idx;
            let start_range_first_idx_to_be_sent = match_idx + 1;
            /*
            match_idx is already included
            match_idx = 1
            last_idx = 3
            0 1 || 2 3
            3 - 1 = 2 elements to be sent
            last idx in drain range is excluded
             */
            let entries_to_append = &self.persistent_state.log[start_range_first_idx_to_be_sent..end_range_first_idx_not_to_be_sent];
            let append_entry = self.get_append_entry(entries_to_append.to_vec(), follower_id);

            self.sender.send(&follower_id, append_entry).await;
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

        if success {
            // last_verified_log_index = entries.len() + prev_index
            // the full length of the follower log
            // the index of the last common entry which is known to reside in the log
            
            // update matchIdx to last verified
            // check the entries length
            // send a batch
            // don't send multiple batches - network errors possible, reordering possible
            self.update_match_idx(source, last_verified_log_index);
            self.send_up_to_batch_size(source).await;
        }
        else {

        }
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
            RaftMessageContent::InstallSnapshot(InstallSnapshotArgs { last_included_index, last_included_term, last_config, client_sessions, offset, data, done }) => {

            },
            RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs { last_included_index, offset }) => {

            },
        }
        todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        let ClientRequest { reply_to, content } = msg;

        match content {
            ClientRequestContent::Command { command, client_id, sequence_num, lowest_sequence_num_without_response } => {

            },
            ClientRequestContent::Snapshot => {

            },
            ClientRequestContent::AddServer { new_server } => {

            },
            ClientRequestContent::RemoveServer { old_server } => {

            },
            ClientRequestContent::RegisterClient => {

            },
        }
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _: ElectionTimeout) {
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