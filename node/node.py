import json
import os
import sys
from datetime import datetime
import random
import hashlib
from enum import Enum
import asyncio
import fastapi
import httpx
import uvicorn
from .logger import logger
from .storage import Storage


class NodeRole(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class Node:
    def store_persistent_state(self, called_from: str):
        temp_file_path = f'{self.node_id}/tmp.txt'
        state = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log
        }
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
            json.dump(state, temp_file)
        os.replace(temp_file_path, self.persistent_file_path)
        logger.info(f'from "{called_from}" called store persistent state: {state}')

    def load_persistent_state(self):
        if os.path.isfile(self.persistent_file_path):
            with open(self.persistent_file_path, encoding='utf-8') as file:
                state = json.load(file)
            self.current_term = state['current_term']
            self.voted_for = state['voted_for']
            self.log = state['log']
            logger.info(f'loaded state: {state}')
        else:
            logger.info('did not find saved persistent state, created default')
            self.store_persistent_state('load_persistent_state')

    def __init__(self, node_id: str, config_path: str):
        self.node_id: str = node_id

        self.persistent_file_path: str = f'{self.node_id}/persistent.txt'
        hashed_node_id = hashlib.sha256(self.node_id.encode('utf-8')).hexdigest()
        random.seed(hashed_node_id)

        self.current_term: int = 0
        self.voted_for: int = 0
        self.log: list[dict[str, any]] = []
        self.load_persistent_state()

        with open(config_path, encoding='utf-8') as file:
            config = json.load(file)
        self.addresses: dict[str, str] = config['addresses']
        self.network_timeout: float = config['network_timeout']
        self.relieved_read: bool = config['relieved_read']
        self.client_request_timeout: float = config['client_request_timeout']
        self.election_delay_lower: float = config['election_delay_lower']
        self.election_delay_upper: float = config['election_delay_upper']
        self.leader_heart_rate: float = config['leader_heart_rate']

        self.commit_length: int = 0
        self.current_role: NodeRole = NodeRole.FOLLOWER
        self.current_leader: str | None = None
        self.votes_received: set[str] = set()
        self.last_heard_from_leader_at = datetime.now()
        self.perform_replication_lock = asyncio.Lock()

        self.sent_length: dict[str, int] = {}
        self.acked_length: dict[str, int] = {}
        self.state_of_operation: dict[int, dict] = {}

        self.storage = Storage(self.log)

        self.app = fastapi.FastAPI(lifespan=self.lifespan)

        self.app.post('/items/{key}')(self.create_item)
        self.app.get('/items/{key}')(self.read_item)
        self.app.put('/items/{key}')(self.update_item)
        self.app.delete('/items/{key}')(self.delete_item)
        self.app.put('/items/{key}/cas')(self.cas_item)

        self.app.post('/raft/vote')(self.raft_vote_request)
        self.app.post('/raft/replicate')(self.raft_replicate_request)

        self.app.get('/debug/state')(self.get_state)

    async def lifespan(self, app: fastapi.FastAPI):
        _ = app
        asyncio.create_task(self.background_election_supervision())
        asyncio.create_task(self.background_replication_supervision())
        logger.info('node has initialized and ready to accept requests')
        yield
        logger.info('shutting down the server')
        sys.exit(0)

    def run(self):
        port = int(self.addresses[self.node_id].split(':')[-1])
        uvicorn.run(self.app, host='0.0.0.0', port=port)

    async def get_state(self):
        return {'state': str(self.__dict__)}

    async def create_item(self, key: str, request: fastapi.Request):
        if self.current_role == NodeRole.LEADER:
            data = await request.json()
            logger.info(f'create request is handling by leader, key: {key}, data: {data}')
            value = data.get("value")
            if value is None:
                raise fastapi.HTTPException(status_code=400, detail="Missing 'value' in request body")
            operation = {
                'type': 'create',
                'key': key,
                'value': value,
            }
            return await self.make_operation(operation)
        if self.current_leader is not None:
            return fastapi.responses.RedirectResponse(
                url=f'http://{self.addresses[self.current_leader]}/items/{key}',
                status_code=307)
        return {'status': 'I do not know who is the leader now'}

    async def read_item(self, key: str):
        operation = {
            'type': 'read',
            'key': key,
        }
        if self.relieved_read:
            logger.info(f'read request is handling in relieved manner, key: {key}')
            return self.storage.apply(operation)
        if self.current_role == NodeRole.LEADER:
            logger.info(f'read request is handling by leader, key: {key}')
            return await self.make_operation(operation)
        if self.current_leader is not None:
            return fastapi.responses.RedirectResponse(
                url=f'http://{self.addresses[self.current_leader]}/items/{key}',
                status_code=307)
        return {'status': 'I do not know who is the leader now'}

    async def update_item(self, key: str, request: fastapi.Request):
        if self.current_role == NodeRole.LEADER:
            data = await request.json()
            logger.info(f'update request is handling by leader, key: {key}, data: {data}')
            value = data.get("value")
            if value is None:
                raise fastapi.HTTPException(status_code=400, detail="Missing 'value' in request body")
            operation = {
                'type': 'update',
                'key': key,
                'value': value,
            }
            return await self.make_operation(operation)
        if self.current_leader is not None:
            return fastapi.responses.RedirectResponse(
                url=f'http://{self.addresses[self.current_leader]}/items/{key}',
                status_code=307)
        return {'status': 'I do not know who is the leader now'}

    async def delete_item(self, key: str):
        if self.current_role == NodeRole.LEADER:
            logger.info(f'delete request is handling by leader, key: {key}')
            operation = {
                'type': 'delete',
                'key': key,
            }
            return await self.make_operation(operation)
        if self.current_leader is not None:
            return fastapi.responses.RedirectResponse(
                url=f'http://{self.addresses[self.current_leader]}/items/{key}',
                status_code=307)
        return {'status': 'I do not know who is the leader now'}

    async def cas_item(self, key: str, request: fastapi.Request):
        if self.current_role == NodeRole.LEADER:
            data = await request.json()
            logger.info(f'cas request is handling by leader, key: {key}, data: {data}')
            if 'expected' not in data:
                raise fastapi.HTTPException(status_code=400, detail="Missing 'expected' in request body")
            expected = data.get("expected")
            if 'desired' not in data:
                raise fastapi.HTTPException(status_code=400, detail="Missing 'desired' in request body")
            desired = data.get("desired")
            operation = {
                'type': 'cas',
                'key': key,
                'expected': expected,
                'desired': desired,
            }
            return await self.make_operation(operation)
        if self.current_leader is not None:
            return fastapi.responses.RedirectResponse(
                url=f'http://{self.addresses[self.current_leader]}/items/{key}/cas',
                status_code=307)
        return {'status': 'I do not know who is the leader now'}

    async def make_operation(self, operation: dict[str, any]):
        self.log.append({'term': self.current_term, 'operation': operation})
        self.store_persistent_state('make_operation')
        self.acked_length[self.node_id] = len(self.log)
        operation_index = len(self.log) - 1
        self.state_of_operation[operation_index] = {'event': asyncio.Event()}
        try:
            await asyncio.wait_for(self.perform_replication(), timeout=self.client_request_timeout)
            await asyncio.wait_for(self.state_of_operation[operation_index]['event'].wait(),
                                   timeout=self.client_request_timeout)
            return self.state_of_operation[operation_index]['result']
        except asyncio.TimeoutError:
            return {'status': 'the operation request timeout has expired on the server'}

    def commit_log_on_leader(self):
        while self.commit_length < len(self.log):
            cnt_acks = 0
            for node_id in self.addresses.keys():
                if self.acked_length[node_id] > self.commit_length:
                    cnt_acks += 1
            if cnt_acks >= len(self.addresses) // 2 + 1:
                operation = self.log[self.commit_length]['operation']
                result = self.storage.apply(operation)
                logger.info(f'leader applied operation: {operation} and received result: {result}')
                if self.commit_length in self.state_of_operation:
                    self.state_of_operation[self.commit_length]['result'] = result
                    self.state_of_operation[self.commit_length]['event'].set()
                self.commit_length += 1

    async def async_broadcast(self, requests: list[tuple[str, dict]], method: str):
        logger.info(f'starting the broadcast with requests: {requests}')
        async with httpx.AsyncClient() as client:
            responses = []
            failures = []

            async def send_request(url: str, json_data: dict):
                try:
                    response = await client.post(url, json=json_data)
                    if response.status_code == 200:
                        return {'status': 'success', 'response': response.json()}
                    return {'status': 'failure', 'error': f'Non-200 status: {response.status_code}'}
                except httpx.RequestError as e:
                    return {'status': 'failure', 'error': str(e)}

            tasks = [
                send_request(f'http://{node_address}/raft/{method}', node_message)
                for node_address, node_message in requests
            ]

            try:
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.network_timeout)
            except asyncio.TimeoutError:
                logger.debug(f'common timeout reached when broadcasting {method}')

        for result in results:
            if result['status'] == 'success':
                responses.append(result['response'])
            else:
                failures.append(result)
        logger.info(f'{method} broadcast has ended, received'
                    f' {len(responses)} responses: {responses}'
                    f' and {len(failures)} failures: {failures}')
        return responses, failures

    async def background_election_supervision(self):
        while True:
            delay = random.uniform(self.election_delay_lower, self.election_delay_upper)
            await asyncio.sleep(delay)
            seconds_since_last_heard = (datetime.now() - self.last_heard_from_leader_at).total_seconds()
            if self.current_role == NodeRole.LEADER or seconds_since_last_heard < 3 * self.leader_heart_rate:
                continue
            logger.info('I suspect the leader is unavailable')
            self.current_role = NodeRole.CANDIDATE
            self.current_leader = None
            self.current_term += 1
            self.voted_for = self.node_id
            self.store_persistent_state('perform_election begin')
            self.votes_received = {self.node_id}
            last_term = self.log[-1]['term'] if len(self.log) > 0 else 0
            message = {
                'node_id': self.node_id,
                'term': self.current_term,
                'log_length': len(self.log),
                'last_term': last_term,
            }
            requests = [
                (node_address, message)
                for node_id, node_address in self.addresses.items()
                if node_id != self.node_id
            ]
            responses, _ = await self.async_broadcast(requests, 'vote')
            for response in responses:
                voter_id = response['node_id']
                term = response['term']
                granted = response['granted']
                if self.current_role == NodeRole.CANDIDATE and self.current_term == term and granted:
                    logger.info(f'in term {self.current_term} received a vote in support from {voter_id}')
                    self.votes_received.add(voter_id)
                    if len(self.votes_received) == len(self.addresses) // 2 + 1:
                        logger.info('was elected as leader')
                        await self.start_leadership()
                        return
                elif self.current_term < term:
                    self.current_term = term
                    self.voted_for = None
                    self.store_persistent_state('perform_election received bigger term')
                    logger.info(f'received newer {term}, become follower')
                    self.current_role = NodeRole.FOLLOWER
                else:
                    logger.debug(f'received garbage message: {response}')

    async def raft_vote_request(self, request: fastapi.Request):
        message = await request.json()
        logger.info(f'raft/vote request received, message: {message}')
        other_id = message['node_id']
        other_term = message['term']
        other_log_length = message['log_length']
        other_log_term = message['last_term']
        self_log_term = self.log[-1]['term'] if len(self.log) > 0 else 0
        log_ok = other_log_term > self_log_term or \
            other_log_term == self_log_term and other_log_length >= len(self.log)
        term_ok = other_term > self.current_term or \
            other_term == self.current_term and self.voted_for in {None, other_id}
        if log_ok and term_ok:
            self.current_term = other_term
            self.voted_for = other_id
            self.store_persistent_state('raft_vote_request')
            self.current_role = NodeRole.FOLLOWER
            granted = True
        else:
            granted = False
        reply = {'node_id': self.node_id, 'term': self.current_term, 'granted': granted}
        logger.info(f'reply: {reply}')
        return reply

    async def start_leadership(self):
        logger.info(f'I am the leader in term {self.current_term}')
        self.current_role = NodeRole.LEADER
        self.current_leader = self.node_id
        self.sent_length = {node_id: len(self.log) for node_id in self.addresses.keys()}
        self.acked_length = {node_id: 0 for node_id in self.addresses.keys()}
        self.acked_length[self.node_id] = len(self.log)
        self.state_of_operation = {}
        await self.perform_replication()

    async def background_replication_supervision(self):
        while True:
            await asyncio.sleep(self.leader_heart_rate)
            if self.current_role != NodeRole.LEADER:
                continue
            await self.perform_replication()

    async def perform_replication(self):
        async with self.perform_replication_lock:
            if self.current_role != NodeRole.LEADER:
                return
            requests = []
            for node_id, node_address in self.addresses.items():
                if node_id == self.node_id:
                    continue
                i = self.sent_length[node_id]
                entries = self.log[i:]
                log_term = self.log[i - 1]['term'] if i > 0 else 0
                message = {
                    'node_id': self.node_id,
                    'term': self.current_term,
                    'log_length': i,
                    'log_term': log_term,
                    'leader_commit': self.commit_length,
                    'entries': entries,
                }
                requests.append((node_address, message))
            responses, _ = await self.async_broadcast(requests, 'replicate')
            for response in responses:
                follower_id = response['node_id']
                term = response['term']
                ack = response['ack']
                success = response['success']
                if term == self.current_term and self.current_role == NodeRole.LEADER:
                    if success:  # do I need (and ack >= self.acked_length[follower_id]) here?
                        assert ack >= self.acked_length[follower_id]
                        self.sent_length[follower_id] = ack
                        self.acked_length[follower_id] = ack
                        self.commit_log_on_leader()
                    elif self.sent_length[follower_id] > 0:
                        self.sent_length[follower_id] -= 1
                        await self.perform_replication()
                elif self.current_term < term:
                    self.current_term = term
                    self.voted_for = None
                    self.store_persistent_state('perform_replication')
                    self.current_role = NodeRole.FOLLOWER

    async def raft_replicate_request(self, request: fastapi.Request):
        message = await request.json()
        logger.info(f'raft/replicate request received, message: {message}')
        leader_id = message['node_id']
        term = message['term']
        log_length = message['log_length']
        log_term = message['log_term']
        leader_commit = message['leader_commit']
        entries = message['entries']
        if self.current_term < term:
            self.current_term = term
            self.voted_for = None
            self.store_persistent_state('raft_replicate_request')
            self.current_role = NodeRole.FOLLOWER
            self.current_leader = leader_id
            logger.debug(f'became a follower of {leader_id} in term {term}')
        if self.current_term == term:
            self.current_role = NodeRole.FOLLOWER
            self.current_leader = leader_id
            logger.debug(f'became a follower of {leader_id} in term {term}')
        if self.current_term == term and self.current_leader == leader_id:
            self.last_heard_from_leader_at = datetime.now()
        log_ok = len(self.log) >= log_length and \
            (log_length == 0 or log_term == self.log[log_length - 1]['term'])
        if term == self.current_term and log_ok:
            self.append_entries(log_length, leader_commit, entries)
            ack = log_length + len(entries)
            success = True
        else:
            ack = 0
            success = False
        reply = {'node_id': self.node_id, 'term': self.current_term, 'ack': ack, 'success': success}
        logger.info(f'reply: {reply}')
        return reply

    def append_entries(self, log_length, leader_commit, entries):
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length]['term'] != entries[0]['term']:
                self.log = self.log[:log_length]
        if len(self.log) < log_length + len(entries):
            self.log += entries[len(self.log) - log_length:]
        while self.commit_length < leader_commit:
            operation = self.log[self.commit_length]['operation']
            result = self.storage.apply(operation)
            logger.info(f'follower applied operation: {operation} and received result: {result}')
            self.commit_length += 1
