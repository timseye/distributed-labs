# Lab 3: Raft Lite Consensus

This project implements a simplified version of the Raft consensus algorithm (Raft Lite) in Python. It supports leader election, log replication, and fault tolerance against crash failures.

## Files

- `node.py`: The Raft Node implementation.
- `client.py`: A client to submit commands to the cluster.

## How to Run Locally

### 1. Start the Cluster
Open 3 terminal windows/tabs and run the following commands to start a 3-node cluster (A, B, C):

**Node A:**
```bash
python3 node.py --id A --port 8000 --peers http://localhost:8001,http://localhost:8002
```

**Node B:**
```bash
python3 node.py --id B --port 8001 --peers http://localhost:8000,http://localhost:8002
```

**Node C:**
```bash
python3 node.py --id C --port 8002 --peers http://localhost:8000,http://localhost:8001
```

### 2. Submit Commands
In a separate terminal, use the client to submit a command. The client will automatically find the leader.

```bash
python3 client.py --peers http://localhost:8000,http://localhost:8001,http://localhost:8002 --cmd "SET x=5"
```

### 3. Check Status
You can check the status of any node using curl (or by visiting the URL in a browser):

```bash
curl http://localhost:8000/status
```

### 4. Failure Experiment (Demo)
1. Start the cluster as above.
2. Submit a command (`SET x=100`).
3. Identify the leader (look for "role": "Leader" in the logs or status).
4. Kill the leader's process (Ctrl+C).
5. Submit a new command (`SET x=200`).
6. Observe that a new leader is elected and the new command is accepted and replicated to the remaining nodes.

## Implementation Details

- **Leader Election:** Uses randomized election timeouts.
- **Log Replication:** AppendEntries RPCs are used for both heartbeats and data replication.
- **Commit Logic:** Entries are committed once replicated to a majority of nodes.
- **Persistence:** Log is kept in memory (volatilte) for this lab.

## Dependencies
- Standard Python 3 library only.
