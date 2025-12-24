import socket
import sys

# Default to "node-b" for Docker, but allow localhost for testing
SERVER_IP = "node-b"
if len(sys.argv) > 1:
    SERVER_IP = sys.argv[1]

PORT = 5000

try:
    s = socket.socket()
    print(f"Connecting to {SERVER_IP}:{PORT}...")
    s.connect((SERVER_IP, PORT))
    s.send(b"Hello from Node A!")
    reply = s.recv(1024).decode()
    print("Server replied:", reply)
    s.close()
except Exception as e:
    print(f"Connection failed: {e}")
