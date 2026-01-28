#!/usr/bin/env python3
"""
Lab 4 Starter — Client (2PC/3PC Transaction Trigger)
=====================================================

Usage:
  python3 client.py --coord http://IP:8000 start TX1 2PC SET x 5
  python3 client.py --coord http://IP:8000 start TX2 3PC TRANSFER balance -100
  python3 client.py --coord http://IP:8000 status
  python3 client.py --participant http://IP:8001 status

Examples:
  # Start 2PC transaction
  python3 client.py --coord http://localhost:8000 start TX123 2PC SET balance 1000

  # Start 3PC transaction
  python3 client.py --coord http://localhost:8000 start TX456 3PC TRANSFER balance -50

  # Check coordinator status
  python3 client.py --coord http://localhost:8000 status

  # Check participant status
  python3 client.py --participant http://localhost:8001 status
"""

from urllib import request
import argparse
import json
import sys
from typing import Any

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def post_json(url: str, payload: dict, timeout: float = 10.0) -> dict:
    """Send POST request with JSON payload"""
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return jload(resp.read())
    except Exception as e:
        return {"ok": False, "error": str(e)}

def get_json(url: str, timeout: float = 10.0) -> dict:
    """Send GET request"""
    try:
        with request.urlopen(url, timeout=timeout) as resp:
            return jload(resp.read())
    except Exception as e:
        return {"ok": False, "error": str(e)}

def start_transaction(coord_url: str, txid: str, protocol: str, op_type: str, key: str, value: str) -> None:
    """Start a transaction via coordinator"""
    
    # Parse value (int or string)
    try:
        parsed_value = int(value)
    except ValueError:
        parsed_value = value

    operation = {
        "type": op_type.upper(),
        "key": key,
        "value": parsed_value
    }

    payload = {
        "txid": txid,
        "protocol": protocol.upper(),
        "op": operation
    }

    print(f"\n{'='*60}")
    print(f"Starting Transaction: {txid}")
    print(f"Protocol: {protocol.upper()}")
    print(f"Operation: {op_type.upper()} {key} = {parsed_value}")
    print(f"{'='*60}\n")

    url = coord_url.rstrip("/") + "/tx/start"
    result = post_json(url, payload)

    if result.get("ok"):
        print(f"✅ Transaction {txid} completed successfully!")
        print(f"   Decision: {result.get('decision')}")
        print(f"   Votes: {json.dumps(result.get('votes', {}), indent=2)}")
    else:
        print(f"❌ Transaction {txid} failed!")
        print(f"   Error: {result.get('error', 'Unknown error')}")

    print(f"\n{'='*60}\n")

def check_status(url: str, node_type: str) -> None:
    """Check status of coordinator or participant"""
    print(f"\n{'='*60}")
    print(f"Checking {node_type} Status: {url}")
    print(f"{'='*60}\n")

    status_url = url.rstrip("/") + "/status"
    result = get_json(status_url)

    if result.get("ok"):
        print(json.dumps(result, indent=2))
    else:
        print(f"❌ Failed to get status: {result.get('error', 'Unknown error')}")

    print(f"\n{'='*60}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Client for Lab 4 Distributed Transactions (2PC/3PC)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start 2PC transaction (SET operation)
  python3 client.py --coord http://localhost:8000 start TX1 2PC SET balance 1000

  # Start 2PC transaction (TRANSFER operation)
  python3 client.py --coord http://localhost:8000 start TX2 2PC TRANSFER balance -100

  # Start 3PC transaction
  python3 client.py --coord http://localhost:8000 start TX3 3PC SET account_x 500

  # Check coordinator status
  python3 client.py --coord http://localhost:8000 status

  # Check participant status
  python3 client.py --participant http://localhost:8001 status
        """
    )

    parser.add_argument("--coord", help="Coordinator base URL (e.g., http://IP:8000)")
    parser.add_argument("--participant", help="Participant base URL (e.g., http://IP:8001)")
    parser.add_argument("command", choices=["start", "status"], help="Command to execute")
    parser.add_argument("args", nargs="*", help="Command arguments")

    args = parser.parse_args()

    # Validate required arguments
    if args.command == "start":
        if not args.coord:
            print("❌ Error: --coord is required for 'start' command")
            sys.exit(1)

        if len(args.args) < 5:
            print("❌ Error: 'start' command requires: TXID PROTOCOL OP_TYPE KEY VALUE")
            print("   Example: start TX1 2PC SET balance 1000")
            sys.exit(1)

        txid, protocol, op_type, key, value = args.args[0], args.args[1], args.args[2], args.args[3], args.args[4]

        if protocol.upper() not in ("2PC", "3PC"):
            print(f"❌ Error: Protocol must be 2PC or 3PC (got: {protocol})")
            sys.exit(1)

        if op_type.upper() not in ("SET", "TRANSFER"):
            print(f"❌ Error: Operation type must be SET or TRANSFER (got: {op_type})")
            sys.exit(1)

        start_transaction(args.coord, txid, protocol, op_type, key, value)

    elif args.command == "status":
        if args.coord:
            check_status(args.coord, "Coordinator")
        elif args.participant:
            check_status(args.participant, "Participant")
        else:
            print("❌ Error: Either --coord or --participant must be specified for 'status' command")
            sys.exit(1)

if __name__ == "__main__":
    main()
