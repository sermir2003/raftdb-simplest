import json
import os
import threading
from enum import Enum
import random
import sys
import requests
from .broadcast import simple_broadcast
from .logger import logger
from .storage import Storage


class NodeRole(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class RaftElectionTimerInvoker:
    def __init__(self, raft_node, config):
        self.raft_node = raft_node
        self.timeout_lower = config['election_timeout_lower']
        self.timeout_upper = config['election_timeout_upper']
        self.timer = threading.Timer(0, self.raft_node.start_elections)

    def cancel(self):
        self.timer.cancel()

    def set(self):
        timeout = random.uniform(self.timeout_lower, self.timeout_upper)
        self.timer = threading.Timer(timeout, self.raft_node.start_elections)
        self.timer.start()

    def reset(self):
        self.cancel()
        self.set()


class RaftReplicateLogTimerInvoker:
    def __init__(self, raft_node, config):
        self.raft_node = raft_node
        self.timeout = config['replicate_log_timeout']
        self.timer = threading.Timer(0, self.raft_node.replicate_log_mutex_wrapped)

    def cancel(self):
        self.timer.cancel()

    def set(self):
        self.timer = threading.Timer(self.timeout, self.raft_node.replicate_log_mutex_wrapped)
        self.timer.start()

    def reset(self):
        self.cancel()
        self.set()


class RaftNode:
    def store_persistent_state(self):
        temp_file_path = f'node-{self.id}-tmp.txt'
        state = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log
        }
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
            json.dump(state, temp_file)
        os.replace(temp_file_path, self.persistent_file_path)

    def load_persistent_state(self):
        if os.path.isfile(self.persistent_file_path):
            with open(self.persistent_file_path, encoding='utf-8') as file:
                state = json.load(file)
            self.current_term = state['current_term']
            self.voted_for = state['voted_for']
            self.log = state['log']
        else:
            self.store_persistent_state()

    def __init__(self, node_id, config_path):
        self.id = node_id

        random.seed(self.id)
        self.persistent_file_path = f'mode-{self.id}-persistent.txt'
        self.mutex = threading.Lock()

        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.load_persistent_state()

        with open(config_path, encoding='utf-8') as file:
            config = json.load(file)
        self.addresses = config['addresses']
        self.network_timeout = config['network_timeout']

        self.election_timer = RaftElectionTimerInvoker(self, config)
        self.replicate_log_timer = RaftReplicateLogTimerInvoker(self, config)

        self.commit_length = 0
        self.current_role = NodeRole.FOLLOWER
        self.current_leader = None
        self.votes_received = set()

        self.sent_length = {}
        self.acked_length = {}

        self.storage = Storage(self.log)
        self.election_timer.set()
        logger.info('ready to serve')

    def start_elections(self):
        # Called by timer, provide thread-safety
        with self.mutex:
            logger.debug('[.] start_elections')
            self.current_role = NodeRole.CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            self.store_persistent_state()
            self.votes_received = {self.id}
            last_term = self.log[-1]['term'] if len(self.log) > 0 else 0
            msg = {
                'msg_type': 'VoteRequest',
                'node_id': self.id,
                'term': self.current_term,
                'log_length': len(self.log),
                'log_term': last_term
            }
            msg_list = [(node_address, msg) for node_id, node_address in self.addresses.items() if node_id != self.id]
            simple_broadcast(msg_list, self.network_timeout)
            self.election_timer.reset()
            logger.debug('[+] start_elections')

    def start_leadership(self):
        self.election_timer.cancel()
        self.current_role = NodeRole.LEADER
        self.current_leader = self.id
        self.sent_length = {node_id: len(self.log) for node_id in self.addresses.keys()}
        self.acked_length = {node_id: 0 for node_id in self.addresses.keys()}
        self.replicate_log()

    def replicate_log(self):
        self.replicate_log_timer.cancel()
        if self.current_role != NodeRole.LEADER:
            return
        msg_list = []
        for node_id, node_address in self.addresses.items():
            if node_id == self.id:
                continue
            i = self.sent_length[node_id]
            entries = self.log[i:]
            log_term = self.log[i - 1]['term'] if i > 0 else 0
            msg = {
                'msg_type': 'LogRequest',
                'node_id': self.id,
                'term': self.current_term,
                'log_length': i,
                'log_term': log_term,
                'leader_commit': self.commit_length,
                'entries': entries,
            }
            msg_list.append((node_address, msg))
        simple_broadcast(msg_list, self.network_timeout)
        self.replicate_log_timer.set()

    def replicate_log_mutex_wrapped(self):
        # Called by timer, provide thread-safety
        with self.mutex:
            self.replicate_log()

    def manage_control_request(self, data):
        # Called by http server, provide thread-safety
        with self.mutex:
            logger.debug('[.] manage_control_request')
            if data['msg_type'] == 'VoteRequest':
                self.on_vote_request(
                    other_id=data['node_id'],
                    other_term=data['term'],
                    other_log_length=data['log_length'],
                    other_log_term=data['log_term'])
            elif data['msg_type'] == 'VoteResponse':
                self.on_vote_response(
                    voter_id=data['node_id'],
                    term=data['term'],
                    granted=data['granted'])
            elif data['msg_type'] == 'LogRequest':
                self.on_log_request(
                    leader_id=data['node_id'],
                    term=data['term'],
                    log_length=data['log_length'],
                    log_term=data['log_term'],
                    leader_commit=data['leader_commit'],
                    entries=data['entries'])
            elif data['msg_type'] == 'LogResponse':
                self.on_log_response(
                    follower_id=data['node_id'],
                    term=data['term'],
                    ack=data['ack'],
                    success=data['success']
                )
            else:
                logger.fatal(f'Unknown incoming msg_type type: {data['msg_type']}')
                sys.exit(1)
            logger.debug('[+] manage_control_request')

    def on_vote_request(self, other_id, other_term, other_log_length, other_log_term):
        logger.debug('[.] on_vote_request')
        self_log_term = self.log[-1]['term'] if len(self.log) > 0 else 0
        log_ok = other_log_term > self_log_term or \
            other_log_term == self_log_term and other_log_length >= len(self.log)
        term_ok = other_term > self.current_term or \
            other_term == self.current_term and self.voted_for in {None, other_id}
        if log_ok and term_ok:
            self.current_term = other_term
            self.current_role = NodeRole.FOLLOWER
            self.voted_for = other_id
            self.store_persistent_state()
            granted = True
            logger.debug('    success')
        else:
            granted = False
            logger.debug('    unsuccess')
        requests.post(
            url=f'http://{self.addresses[other_id]}/raft_control',
            json={
                'msg_type': 'VoteResponse',
                'node_id': self.id,
                'term': self.current_term,
                'granted': granted,
            },
            timeout=self.network_timeout
        )
        logger.debug('[+] on_vote_request')

    def on_vote_response(self, voter_id, term, granted):
        logger.debug('[.] on_vote_response')
        if self.current_role == NodeRole.CANDIDATE and self.current_term == term and granted:
            logger.debug('    Received a vote in support')
            self.votes_received.add(voter_id)
            if len(self.votes_received) >= len(self.addresses) // 2 + 1:
                logger.debug('    Was elected as leader')
                self.start_leadership()
        elif self.current_term < term:
            self.current_term = term
            self.voted_for = None
            self.store_persistent_state()
            self.current_role = NodeRole.FOLLOWER
            self.election_timer.reset()
            logger.debug('    Received newer term, become follower')
        else:
            logger.debug('    Received garbage message')
        logger.debug('[+] on_vote_response')

    def on_log_request(self, leader_id, term, log_length, log_term, leader_commit, entries):
        logger.debug('[.] on_log_request')
        if self.current_term < term:
            self.current_term = term
            self.voted_for = None
            self.store_persistent_state()
            self.current_role = NodeRole.FOLLOWER
            self.election_timer.reset()
            self.current_leader = leader_id
            logger.debug(f'    From obsolete term became a follower of {leader_id}')
        if self.current_term == term and self.current_role == NodeRole.CANDIDATE:
            self.current_role = NodeRole.FOLLOWER
            self.election_timer.reset()
            self.current_leader = leader_id
            logger.debug(f'    From candidate become a follower of {leader_id} in the same term {term}')
        log_ok = len(self.log) >= log_length and \
            (log_length == 0 or log_term == self.log[log_length - 1]['term'])
        if term == self.current_term and log_ok:
            self.append_entries(log_length, leader_commit, entries)
            ack = log_length + len(entries)
            success = True
            logger.debug('    Succeed')
        else:
            ack = 0
            success = False
            logger.debug('    Failed')
        requests.post(
            url=f'http://{self.addresses[leader_id]}/raft_control',
            json={
                'msg_type': 'LogResponse',
                'node_id': self.id,
                'term': self.current_term,
                'ack': ack,
                'success': success,
            },
            timeout=self.network_timeout
        )
        logger.debug('[+] on_log_request')

    def on_log_response(self, follower_id, term, ack, success):
        logger.debug('[.] on_log_response')
        if term == self.current_term and self.current_role == NodeRole.LEADER:
            if success and ack >= self.acked_length[follower_id]:
                self.sent_length[follower_id] = ack
                self.acked_length[follower_id] = ack
                self.commit_log_entries()
                logger.debug('    Received success')
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] -= 1
                logger.debug('    Received unsuccess')
                self.replicate_log()
        elif self.current_term < term:
            self.current_term = term
            self.current_role = NodeRole.FOLLOWER
            self.election_timer.reset()
            self.voted_for = None
        logger.debug('[+] on_log_response')

    def on_change_request(self, change):
        # Called by http server, provide thread-safety
        with self.mutex:
            logger.debug('[.] on_change_request')
            if self.current_role == NodeRole.LEADER:
                self.log.append({
                    'term': self.current_term,
                    'change': change
                })
                self.store_persistent_state()
                self.acked_length[self.id] = len(self.log)
                self.replicate_log()
                logger.debug('[+] on_change_request, change has accepted')
                return {'message': 'ok'}
            if self.current_leader is not None:
                logger.debug(f'[+] on_change_request, redirecting to node {self.current_leader}')
                return {'message': 'redirect', 'url': f'http://{self.addresses(self.current_leader)}'}
            logger.debug('[+] on_change_request, Do not know who is Mr Leader')
            return {'message': 'Do not know who is Mr Leader'}

    def on_read_request(self, key):
        # Called by http server, provide thread-safety
        with self.mutex:
            return self.storage.read(key)

    def append_entries(self, log_length, leader_commit, entries):
        logger.debug('[.] append_entries')
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length]['term'] != entries[0]['term']:
                self.log = self.log[:log_length]
                logger.debug('    Trimmed self.log')
        if len(self.log) < log_length + len(entries):
            self.log += entries[len(self.log) - log_length:]
        while self.commit_length < leader_commit:
            change = self.log[self.commit_length]
            self.storage.apply(change)
            self.commit_length += 1
        self.store_persistent_state()
        logger.debug('[+] append_entries')

    def commit_log_entries(self):
        logger.debug('[.] commit_log_entries')
        while self.commit_length < len(self.log):
            cnt_acks = 0
            for node_id in self.addresses.keys():
                if self.acked_length[node_id] > self.commit_length:
                    cnt_acks += 1
            if cnt_acks >= len(self.addresses) / 2 + 1:
                change = self.log[self.commit_length]
                self.storage.apply(change)
                self.commit_length += 1
            else:
                self.store_persistent_state()
                logger.debug('[+] commit_log_entries')
                break
