use std::time::SystemTime;

use module_system::{Handler, ModuleRef, System, TimerHandle};
use uuid::Uuid;
use std::collections::HashSet;
use std::future::Future;
use tokio::time::Duration;

pub use domain::*;

mod domain;

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
    granted_votes: HashSet<Uuid>,
    // sending_set: HashSet<Uuid>,
    election_timer: Option<TimerHandle>,
    enabled: bool,
    self_ref: Option<ModuleRef<Self>>,
    heartbeat_timer: Option<TimerHandle>,
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

    fn get_last_log_and_idx(&self) -> (Option<&LogEntry>, usize)  {
        return (self.persistent_state.log.last(), self.persistent_state.log.len());
    }

    fn is_other_log_at_least_as_up_to_date_as_self(&self, last_log_index: usize, last_log_term: u64) -> bool {
        // let self_last_log_idx
        let (self_log, self_idx) = self.get_last_log_and_idx();

        match self_log {
            // idx zero for empty log
            None =>  {
            // the sent log has either no elements (as our log),
            // thus it is as up-to-date as ours
            // or can have any entry, which is more up-to-date as ours
                return true;
            },

            Some(log) => {
                let self_term = log.term;
                
                if last_log_term > self_term {
                    // our term is older
                    return true;
                }
                else { 
                    return (self_term == last_log_term) && (self_idx <= last_log_index);
                }
            }
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

    async fn convert_to_follower_if_term_newer(&mut self, current_term: u64) {
        // safety check
        if self.persistent_state.current_term < current_term {
            self.persistent_state.current_term = current_term;
            self.update_storage().await;

            self.persistent_state.voted_for = None;
            self.update_storage().await;

            self.leader_id = None;
        }
    }
    
    async fn reset_timer(&mut self, interval: Duration) {
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
            if self.persistent_state.current_term < term {
                // convert to a follower

            }
            // if log is at least as up-to-date as mine - grant vote, update your vote
            match self.persistent_state.voted_for {
                None => {
                    if self.is_other_log_at_least_as_up_to_date_as_self(last_log_index, last_log_term) {
                        self.convert_to_follower_if_term_newer(last_log_term).await;
                        self.update_candidate(source).await;
                    }
                }, 
                Some(_candidate_id) => {
                    if self.is_other_log_at_least_as_up_to_date_as_self(last_log_index, last_log_term) {

                    }
                },
            }
        }
    }

    async fn handle_request_response(&mut self, request_response: RequestVoteResponseArgs) {

    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        let RaftMessage { header, content } = msg;

        match content {
            RaftMessageContent::AppendEntries(AppendEntriesArgs { prev_log_index, prev_log_term, entries, leader_commit }) => {

            },
            RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs { success, last_verified_log_index }) => {

            },
            RaftMessageContent::RequestVote(request_vote_args) => {
                self.handle_request_vote(request_vote_args, header).await;
            },
            RaftMessageContent::RequestVoteResponse(request_vote_response) => {
                self.handle_request_response(request_vote_response).await;
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