#!/usr/bin/env python3

import argparse
import json
import threading
import time
import random
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, error
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple

NODE_ID = ""
PEERS = []
HTTP_PORT = 8000

ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
HEARTBEAT_INTERVAL = 0.5

class Role(Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"

class RaftNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.lock = threading.RLock()
        
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[Dict] = []
        
        self.commit_index = -1
        self.last_applied = -1
        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.last_heartbeat_time = time.time()
        self.election_timeout = self._reset_election_timeout()
        
        self.running = True
        self.timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self.timer_thread.start()

    def _reset_election_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def send_rpc(self, url: str, endpoint: str, data: Dict) -> Optional[Dict]:
        full_url = f"{url}/{endpoint}"
        try:
            req = request.Request(
                full_url,
                data=json.dumps(data).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with request.urlopen(req, timeout=0.5) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            return None

    def _timer_loop(self):
        while self.running:
            time.sleep(0.1)
            with self.lock:
                now = time.time()
                
                if self.role == Role.LEADER:
                    if now - self.last_heartbeat_time >= HEARTBEAT_INTERVAL:
                        self.last_heartbeat_time = now
                        threading.Thread(target=self.replicate_log, daemon=True).start()
                else:
                    if now - self.last_heartbeat_time >= self.election_timeout:
                        print(f"[{self.node_id}] Election timeout! Becoming Candidate.")
                        self.start_election()

    def start_election(self):
        with self.lock:
            self.role = Role.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat_time = time.time()
            self.election_timeout = self._reset_election_timeout()
            print(f"[{self.node_id}] Starting election for term {self.current_term}")
            
            term_at_election = self.current_term
        
        votes_received = 1
        
        def request_vote_worker(peer_url):
            nonlocal votes_received
            resp = self.send_rpc(peer_url, 'request_vote', {
                "term": term_at_election,
                "candidate_id": self.node_id
            })
            
            if resp:
                with self.lock:
                    if resp['term'] > self.current_term:
                        self.current_term = resp['term']
                        self.role = Role.FOLLOWER
                        self.voted_for = None
                        return

                    if self.role != Role.CANDIDATE or self.current_term != term_at_election:
                        return
                        
                    if resp['vote_granted']:
                        votes_received += 1
                        print(f"[{self.node_id}] Vote received from {peer_url}. Total: {votes_received}")
                        
                        if votes_received > (len(self.peers) + 1) // 2:
                            if self.role != Role.LEADER:
                                print(f"[{self.node_id}] Majority reached! Becoming LEADER.")
                                self.role = Role.LEADER
                                self.leader_id = self.node_id
                                for peer in self.peers:
                                    self.next_index[peer] = len(self.log)
                                    self.match_index[peer] = -1
                                self.replicate_log()

        for peer in self.peers:
            threading.Thread(target=request_vote_worker, args=(peer,), daemon=True).start()

    def replicate_log(self):
        with self.lock:
            if self.role != Role.LEADER:
                return
            term = self.current_term
            leader_id = self.node_id
        
        def replicate_worker(peer_url):
            with self.lock:
                if self.role != Role.LEADER: return
                prev_idx = self.next_index.get(peer_url, 0) - 1
                prev_term = -1
                if prev_idx >= 0 and prev_idx < len(self.log):
                    prev_term = self.log[prev_idx]['term']
                
                entries = self.log[self.next_index.get(peer_url, 0):]
            
            resp = self.send_rpc(peer_url, 'append_entries', {
                "term": term,
                "leader_id": leader_id,
                "entries": entries,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "leader_commit": self.commit_index
            })
            
            if resp:
                with self.lock:
                    if resp['term'] > self.current_term:
                        print(f"[{self.node_id}] Higher term discovered. Stepping down.")
                        self.current_term = resp['term']
                        self.role = Role.FOLLOWER
                        self.voted_for = None
                        return

                    if self.role != Role.LEADER: return

                    if resp['success']:
                        if entries:
                            new_match = prev_idx + len(entries)
                            self.match_index[peer_url] = max(self.match_index.get(peer_url, -1), new_match)
                            self.next_index[peer_url] = self.match_index[peer_url] + 1
                            self.update_commit_index()
                    else:
                        self.next_index[peer_url] = max(0, self.next_index.get(peer_url, 0) - 1)

        for peer in self.peers:
            threading.Thread(target=replicate_worker, args=(peer,), daemon=True).start()

    def update_commit_index(self):
        indexes = sorted(self.match_index.values())
        indexes.append(len(self.log) - 1)
        indexes.sort()
        
        majority_idx = indexes[(len(self.peers) + 1) // 2]
        
        if majority_idx > self.commit_index:
             if majority_idx < len(self.log) and self.log[majority_idx]['term'] == self.current_term:
                 self.commit_index = majority_idx
                 print(f"[{self.node_id}] Commit index updated to {self.commit_index}")

    def handle_submit(self, command: str) -> bool:
        with self.lock:
            if self.role != Role.LEADER:
                return False
            self.log.append({"term": self.current_term, "command": command})
            print(f"[{self.node_id}] Log appended: {command} at index {len(self.log)-1}")
            self.replicate_log()
            return True

    def handle_request_vote(self, term: int, candidate_id: str) -> Tuple[int, bool]:
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.role = Role.FOLLOWER
                self.voted_for = None
                self.leader_id = None
            
            granted = False
            if term < self.current_term:
                granted = False
            elif (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.last_heartbeat_time = time.time()
                granted = True
                
            return self.current_term, granted

    def handle_append_entries(self, term: int, leader_id: str, entries: List[Dict], prev_log_index: int, prev_log_term: int, leader_commit: int) -> Tuple[int, bool]:
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.role = Role.FOLLOWER
                self.voted_for = None
            
            if term == self.current_term:
                self.role = Role.FOLLOWER
                self.leader_id = leader_id
                self.last_heartbeat_time = time.time()

            if term < self.current_term:
                return self.current_term, False

            if prev_log_index >= 0:
                if prev_log_index >= len(self.log):
                    return self.current_term, False
                if self.log[prev_log_index]['term'] != prev_log_term:
                    self.log = self.log[:prev_log_index]
                    return self.current_term, False

            insert_idx = prev_log_index + 1
            for entry in entries:
                if insert_idx < len(self.log):
                    if self.log[insert_idx]['term'] != entry['term']:
                        self.log = self.log[:insert_idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
                insert_idx += 1
            
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
            return self.current_term, True

    def get_status(self):
        with self.lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leader": self.leader_id,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "log": self.log
            }

raft_node: Optional[RaftNode] = None

class HTTPHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: Dict):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
        except Exception:
            self._send_json(400, {"error": "Invalid JSON"})
            return

        if self.path == '/request_vote':
            term = data.get('term')
            candidate_id = data.get('candidate_id')
            current_term, granted = raft_node.handle_request_vote(term, candidate_id)
            self._send_json(200, {"term": current_term, "vote_granted": granted})
        
        elif self.path == '/append_entries':
            term = data.get('term')
            leader_id = data.get('leader_id')
            entries = data.get('entries', [])
            prev_index = data.get('prev_log_index', -1)
            prev_term = data.get('prev_log_term', -1)
            leader_commit = data.get('leader_commit', -1)
            current_term, success = raft_node.handle_append_entries(term, leader_id, entries, prev_index, prev_term, leader_commit)
            self._send_json(200, {"term": current_term, "success": success})
            
        elif self.path == '/submit':
            command = data.get('command')
            if not command:
                self._send_json(400, {"error": "Command required"})
                return
            
            success = raft_node.handle_submit(command)
            if success:
                self._send_json(200, {"status": "submitted", "leader": raft_node.node_id})
            else:
                self._send_json(503, {"error": "Not Leader", "leader": raft_node.leader_id})
            
        else:
            self._send_json(404, {"error": "Not found"})

    def do_GET(self):
        if self.path == '/status':
            status = raft_node.get_status()
            self._send_json(200, status)
        else:
            self._send_json(404, {"error": "Not found"})
    
    def log_message(self, fmt, *args):
        pass

def main():
    global raft_node, NODE_ID, HTTP_PORT, PEERS
    parser = argparse.ArgumentParser(description='Raft Lite Node')
    parser.add_argument('--id', required=True, help='Node ID (e.g., A)')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on')
    parser.add_argument('--peers', required=True, help='Comma-separated peer URLs (e.g., http://localhost:8001,http://localhost:8002)')
    
    args = parser.parse_args()
    NODE_ID = args.id
    HTTP_PORT = args.port
    PEERS = [p for p in args.peers.split(',') if p]

    raft_node = RaftNode(NODE_ID, PEERS)

    print(f"[{NODE_ID}] Raft Node starting on port {HTTP_PORT}")
    print(f"[{NODE_ID}] Peers: {PEERS}")

    server = ThreadingHTTPServer(('0.0.0.0', HTTP_PORT), HTTPHandler)
    server.serve_forever()

if __name__ == '__main__':
    main()