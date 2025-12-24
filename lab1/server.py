import socket
import json
import time
import sys

def add(a, b):
    return a + b

def start_server(delay=False):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 5000))
    s.listen(5)
    print("RPC Server is running on port 5000...")

    while True:
        conn, addr = s.accept()
        try:
            data = conn.recv(1024).decode()
            if not data: continue
            
            req = json.loads(data)
            print(f"LOG: Received request {req['request_id']} for method '{req['method']}'")

            if delay:
                print(f"[SIMULATING DELAY] Server sleeping for 3 seconds...")
                time.sleep(3)
                print(f"[DELAY COMPLETE] Resuming processing...")

            if req['method'] == 'add':
                res_val = add(req['params']['a'], req['params']['b'])
                response = {
                    "request_id": req['request_id'],
                    "result": res_val,
                    "status": "OK"
                }
            else:
                response = {"status": "ERROR", "message": "Method not found"}

            conn.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    delay_mode = "--delay" in sys.argv
    if delay_mode:
        print("[MODE] Running with 3-second delay simulation")
    start_server(delay=delay_mode)