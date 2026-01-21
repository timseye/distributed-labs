#!/usr/bin/env python3

import argparse
import json
import time
from urllib import request, error
from typing import List, Optional

def send_command(peer_url: str, command: str) -> Optional[dict]:
    url = f"{peer_url}/submit"
    data = json.dumps({"command": command}).encode('utf-8')
    req = request.Request(url, data=data, headers={'Content-Type': 'application/json'}, method='POST')
    
    try:
        with request.urlopen(req, timeout=2.0) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except error.HTTPError as e:
        if e.code == 503:
            try:
                body = e.read().decode('utf-8')
                return json.loads(body)
            except:
                pass
    except Exception as e:
        pass
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peers', required=True, help='Comma-separated peer URLs')
    parser.add_argument('--cmd', required=True, help='Command to submit (e.g., "SET x=5")')
    args = parser.parse_args()

    peers = [p.strip() for p in args.peers.split(',') if p.strip()]
    command = args.cmd

    print(f"Submitting '{command}' to cluster {peers}...")

    for _ in range(3):
        for peer in peers:
            print(f"Trying {peer}...")
            resp = send_command(peer, command)
            
            if resp:
                if resp.get('status') == 'submitted':
                    print(f"SUCCESS: Command accepted by Leader {resp.get('leader')}")
                    return
                elif 'error' in resp and resp['error'] == 'Not Leader':
                    leader_hint = resp.get('leader')
                    print(f"  -> Not Leader. Hint: {leader_hint}")
                    continue
    
    print("FAILURE: Could not submit command to any leader.")

if __name__ == '__main__':
    main()
