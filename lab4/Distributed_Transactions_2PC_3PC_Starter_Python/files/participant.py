#!/usr/bin/env python3
"""
Lab 4 Starter — Participant (2PC/3PC) (HTTP, standard library only)
====================================================================

Endpoints (JSON):
- POST /prepare       {"txid":"TX1","op":{"type":"SET","key":"x","value":"5"}}  → {"vote":"YES"|"NO"}
- POST /commit        {"txid":"TX1"}  → {"status":"COMMITTED"}
- POST /abort         {"txid":"TX1"}  → {"status":"ABORTED"}
- POST /can_commit    {"txid":"TX1","op":{...}}  → {"vote":"YES"|"NO"}
- POST /precommit     {"txid":"TX1"}  → {"status":"PRECOMMIT"}
- GET  /status        → {"ok":true, "node":"B", "transactions":{...}, "resources":{...}}

States: INIT → READY → COMMITTED (2PC)
        INIT → PRECOMMIT → COMMITTED (3PC)
             ↓
           ABORTED

WAL format:
TX1 INIT SET {"type":"SET","key":"x","value":5}
TX1 READY
TX1 COMMITTED
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import os
import threading
import time
from typing import Dict, Any, Optional

# === States ===
STATE_INIT = "INIT"
STATE_READY = "READY"
STATE_PRECOMMIT = "PRECOMMIT"
STATE_COMMITTED = "COMMITTED"
STATE_ABORTED = "ABORTED"

# === Global State ===
NODE_ID: str = ""
PORT: int = 8001
WAL_PATH: Optional[str] = None
lock = threading.Lock()

# Transaction state: txid -> {"state": "READY", "op": {...}}
transactions: Dict[str, Dict[str, Any]] = {}

# Resources (key-value store): key -> value
resources: Dict[str, Any] = {}

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def wal_append(line: str) -> None:
    """Append line to WAL with fsync for durability"""
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno())  # Ensure data is written to disk (durability!)

def replay_wal() -> None:
    """Replay WAL on startup to recover state"""
    if not WAL_PATH or not os.path.exists(WAL_PATH):
        return

    print(f"[{NODE_ID}] Replaying WAL from {WAL_PATH}")
    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                parts = line.split(" ", 3)  # txid state op_type op_json
                if len(parts) < 2:
                    continue

                txid = parts[0]
                state = parts[1]

                if state in (STATE_INIT, STATE_READY, STATE_PRECOMMIT):
                    # Extract operation if present
                    if len(parts) >= 4:
                        op_type = parts[2]
                        op_json = parts[3]
                        op = json.loads(op_json)
                        transactions[txid] = {"state": state, "op": op}
                    else:
                        transactions[txid] = {"state": state}
                    print(f"[{NODE_ID}] Replayed {txid} → {state}")

                elif state == STATE_COMMITTED:
                    # Apply the operation if we have it
                    if txid in transactions and "op" in transactions[txid]:
                        apply_operation(transactions[txid]["op"])
                    transactions[txid] = {"state": STATE_COMMITTED}
                    print(f"[{NODE_ID}] Replayed {txid} → COMMITTED (applied)")

                elif state == STATE_ABORTED:
                    transactions[txid] = {"state": STATE_ABORTED}
                    print(f"[{NODE_ID}] Replayed {txid} → ABORTED")

            except Exception as e:
                print(f"[{NODE_ID}] WAL replay error: {e} (line: {line})")

    print(f"[{NODE_ID}] WAL replay complete. Transactions: {transactions}, Resources: {resources}")

def validate_operation(op: dict) -> bool:
    """Validate if operation can be performed"""
    op_type = op.get("type", "").upper()
    key = op.get("key")
    value = op.get("value")

    if op_type == "SET":
        return key is not None and value is not None

    elif op_type == "TRANSFER":
        # For TRANSFER, check if we have enough balance
        if key is None or value is None:
            return False
        current = resources.get(key, 0)
        # If transfer would result in negative balance, reject
        try:
            new_value = current + int(value)
            if new_value < 0:
                return False
        except (ValueError, TypeError):
            return False
        return True

    # Unknown operation type
    return False

def apply_operation(op: dict) -> None:
    """Apply operation to local resources"""
    op_type = op.get("type", "").upper()
    key = op.get("key")
    value = op.get("value")

    if op_type == "SET":
        resources[key] = value
        print(f"[{NODE_ID}] Applied SET {key} = {value}")

    elif op_type == "TRANSFER":
        current = resources.get(key, 0)
        resources[key] = current + int(value)
        print(f"[{NODE_ID}] Applied TRANSFER {key}: {current} + {value} = {resources[key]}")

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {
                    "ok": True,
                    "node": NODE_ID,
                    "port": PORT,
                    "transactions": transactions,
                    "resources": resources
                })
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = jload(raw)
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        txid = body.get("txid", "").strip()
        if not txid:
            self._send(400, {"ok": False, "error": "txid required"})
            return

        # === 2PC: PREPARE ===
        if self.path == "/prepare":
            op = body.get("op")
            if not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "op required"})
                return

            with lock:
                print(f"[{NODE_ID}] TX{txid} PREPARE received")

                # Check if transaction already exists
                if txid in transactions:
                    print(f"[{NODE_ID}] TX{txid} VOTE-NO (already exists)")
                    self._send(200, {"vote": "NO", "reason": "Transaction already exists"})
                    return

                # Validate operation
                if not validate_operation(op):
                    print(f"[{NODE_ID}] TX{txid} VOTE-NO (validation failed)")
                    transactions[txid] = {"state": STATE_ABORTED, "op": op}
                    wal_append(f"{txid} {STATE_ABORTED}")
                    self._send(200, {"vote": "NO", "reason": "Validation failed"})
                    return

                # Transition to READY state (2PC)
                transactions[txid] = {"state": STATE_READY, "op": op}
                wal_append(f"{txid} {STATE_READY} {op.get('type','')} {json.dumps(op)}")
                print(f"[{NODE_ID}] TX{txid} VOTE-YES (state → READY)")
                self._send(200, {"vote": "YES"})
            return

        # === 2PC: COMMIT ===
        if self.path == "/commit":
            with lock:
                print(f"[{NODE_ID}] TX{txid} GLOBAL-COMMIT received")

                if txid not in transactions:
                    # Idempotent: already committed or unknown
                    print(f"[{NODE_ID}] TX{txid} COMMIT (unknown tx, idempotent)")
                    self._send(200, {"status": "COMMITTED"})
                    return

                tx = transactions[txid]
                current_state = tx.get("state")

                # Can only commit from READY or PRECOMMIT state
                if current_state not in (STATE_READY, STATE_PRECOMMIT):
                    print(f"[{NODE_ID}] TX{txid} COMMIT-ERROR (invalid state: {current_state})")
                    self._send(400, {"ok": False, "error": f"Invalid state: {current_state}"})
                    return

                # Apply operation
                if "op" in tx:
                    apply_operation(tx["op"])

                # Transition to COMMITTED
                transactions[txid]["state"] = STATE_COMMITTED
                wal_append(f"{txid} {STATE_COMMITTED}")
                print(f"[{NODE_ID}] TX{txid} COMMITTED")
                self._send(200, {"status": "COMMITTED"})
            return

        # === 2PC: ABORT ===
        if self.path == "/abort":
            with lock:
                print(f"[{NODE_ID}] TX{txid} GLOBAL-ABORT received")

                if txid not in transactions:
                    # Idempotent: already aborted or unknown
                    transactions[txid] = {"state": STATE_ABORTED}

                transactions[txid]["state"] = STATE_ABORTED
                wal_append(f"{txid} {STATE_ABORTED}")
                print(f"[{NODE_ID}] TX{txid} ABORTED")
                self._send(200, {"status": "ABORTED"})
            return

        # === 3PC: CAN-COMMIT ===
        if self.path == "/can_commit":
            op = body.get("op")
            if not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "op required"})
                return

            with lock:
                print(f"[{NODE_ID}] TX{txid} CAN-COMMIT received")

                if txid in transactions:
                    print(f"[{NODE_ID}] TX{txid} VOTE-NO (already exists)")
                    self._send(200, {"vote": "NO", "reason": "Transaction already exists"})
                    return

                if not validate_operation(op):
                    print(f"[{NODE_ID}] TX{txid} VOTE-NO (validation failed)")
                    transactions[txid] = {"state": STATE_ABORTED, "op": op}
                    wal_append(f"{txid} {STATE_ABORTED}")
                    self._send(200, {"vote": "NO", "reason": "Validation failed"})
                    return

                # Transition to INIT state (3PC phase 1)
                transactions[txid] = {"state": STATE_INIT, "op": op}
                wal_append(f"{txid} {STATE_INIT} {op.get('type','')} {json.dumps(op)}")
                print(f"[{NODE_ID}] TX{txid} VOTE-YES (can-commit)")
                self._send(200, {"vote": "YES"})
            return

        # === 3PC: PRE-COMMIT ===
        if self.path == "/precommit":
            with lock:
                print(f"[{NODE_ID}] TX{txid} PRE-COMMIT received")

                if txid not in transactions:
                    print(f"[{NODE_ID}] TX{txid} PRE-COMMIT-ERROR (unknown tx)")
                    self._send(400, {"ok": False, "error": "Unknown transaction"})
                    return

                tx = transactions[txid]
                current_state = tx.get("state")

                if current_state != STATE_INIT:
                    print(f"[{NODE_ID}] TX{txid} PRE-COMMIT-ERROR (invalid state: {current_state})")
                    self._send(400, {"ok": False, "error": f"Invalid state: {current_state}"})
                    return

                # Transition to PRECOMMIT state
                transactions[txid]["state"] = STATE_PRECOMMIT
                wal_append(f"{txid} {STATE_PRECOMMIT}")
                print(f"[{NODE_ID}] TX{txid} PRE-COMMITTED (state → PRECOMMIT)")
                self._send(200, {"status": "PRECOMMIT"})
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        # Suppress default HTTP logs
        return

def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="Participant ID (e.g., B, C, D)")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True, help="Port to listen on")
    ap.add_argument("--wal", default="", help="Optional WAL path (e.g., /tmp/participant_B.wal)")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal.strip() or None

    # Replay WAL to recover state
    replay_wal()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[{NODE_ID}] Participant listening on {args.host}:{args.port}")
    print(f"[{NODE_ID}] WAL: {WAL_PATH or 'disabled'}")
    print(f"[{NODE_ID}] Ready to accept transactions")
    server.serve_forever()

if __name__ == "__main__":
    main()
