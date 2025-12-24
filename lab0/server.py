import socket
import time

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 5000

s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((HOST, PORT))
s.listen(1)
print("Server ready on port 5000...")

while True:
    try:
        conn, addr = s.accept()
        data = conn.recv(1024).decode()
        print(f"Received: {data} from {addr}")
        
        # Challenge 1: Simulate delay (Uncomment to test)
        # time.sleep(3)
        
        conn.send(b"Message received!")
        conn.close()
    except KeyboardInterrupt:
        print("\nServer stopping...")
        break
    except Exception as e:
        print(f"Error: {e}")
