Lab 4: Distributed Transactions (2PC and 3PC)
Implementation of Two-Phase Commit and Three-Phase Commit protocols for distributed transactions on an AWS EC2 cluster.

Files
coordinator.py: Manages transaction lifecycle and participant voting.

participant.py: Manages local resources and executes operations.

client.py: Triggers transactions and queries node status.

Quick Start
1. Launch EC2 Instances
Launch 3 Ubuntu instances (22.04 or 24.04). Ensure Security Groups allow TCP ports 8000-8003.

Instance 1: Coordinator

Instance 2: Participant B

Instance 3: Participant C

2. Prepare Environment
Install Python 3 on all nodes:

Bash

sudo apt update && sudo apt install python3 -y
Upload the Python scripts to the home directory of each instance.

3. Start Participants
Run on Instance 2:

Bash

python3 participant.py --id B --port 8001 --wal /tmp/participant_B.wal
Run on Instance 3:

Bash

python3 participant.py --id C --port 8002 --wal /tmp/participant_C.wal
4. Start Coordinator
Run on Instance 1 (replace IPs with actual private or public IPs):

Bash

python3 coordinator.py --id COORD --port 8000 \
  --participants http://IP_B:8001,http://IP_C:8002 \
  --wal /tmp/coordinator.wal
Testing Scenarios
Scenario 1: Standard 2PC Success
Bash

python3 client.py --coord http://COORD_IP:8000 start TX1 2PC SET balance 1000
This demonstrates the PREPARE -> VOTE -> COMMIT flow.

Scenario 2: 2PC Blocking (Coordinator Failure)
Start a transaction using the client.

Terminate the coordinator process (pkill -9 -f coordinator.py) during the execution delay.

Check participant status: python3 client.py --participant http://IP_B:8001 status.

Observation: Participant remains in READY state, demonstrating the 2PC blocking limitation.

Scenario 3: Recovery via WAL
After Scenario 2, restart the coordinator on Instance 1.

Observation: The coordinator reads /tmp/coordinator.wal, identifies the pending decision, and re-broadcasts the COMMIT to participants.

Scenario 4: Standard 3PC Success
Bash

python3 client.py --coord http://COORD_IP:8000 start TX2 3PC SET data 777
This demonstrates the three phases: CAN-COMMIT, PRE-COMMIT, and DO-COMMIT.

Write-Ahead Log (WAL)
The system uses WAL to ensure durability and atomicity:

Coordinator: Logs the final decision before notifying participants.

Participant: Logs its vote and state (READY/COMMITTED) to disk.

Recovery: On restart, nodes replay the WAL to resolve unfinished transactions.

Features
Atomic Commitment: Transactions either commit on all nodes or none.

Fault Tolerance: Handles participant crashes via timeouts and coordinator crashes via WAL.

Validation: Supports logic checks (e.g., preventing negative balances).

Transparency: Client tool provides real-time state visualization of the cluster.

Troubleshooting
Connection Refused: Verify participants are running and security groups allow the ports.

Stuck READY state: This is the expected behavior in 2PC when the coordinator fails. Restart the coordinator to unblock.

Fresh Start: To reset the system state entirely, delete the WAL files: rm /tmp/*.wal.