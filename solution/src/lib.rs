use std::time::SystemTime;

use module_system::{Handler, ModuleRef, System};
use uuid::Uuid;
use std::collections::HashSet;
use std::future::Future;

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
    // sending_set: HashSet<Uuid>,
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

    fn handle_request_vote(&mut self, request_vote: RequestVoteArgs, request_header: RaftMessageHeader) {
        let RequestVoteArgs { last_log_index, last_log_term } = request_vote;
        let RaftMessageHeader { source, term } = request_header;
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
                self.handle_request_vote(request_vote_args, header);
            },
            RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs { vote_granted }) => {

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
