import socket
import json
import uuid
import time

SERVER_IP = "3.87.133.81" 
PORT = 5000

def rpc_call(method, params):
    request_id = str(uuid.uuid4())[:8]
    payload = {
        "request_id": request_id,
        "method": method,
        "params": params,
        "timestamp": time.time()
    }

    retries = 3
    for attempt in range(retries):
        try:
            print(f"[{time.strftime('%H:%M:%S')}] Sending {method} (ID: {request_id}), attempt {attempt + 1}...")
            
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            s.settimeout(2.0) 
            
            s.connect((SERVER_IP, PORT))
            s.sendall(json.dumps(payload).encode())
            
            data = s.recv(1024).decode()
            response = json.loads(data)
            s.close()
            return response
            
        except socket.timeout:
            print(f"TIMEOUT: Server did not respond within 2 seconds.")
        except Exception as e:
            print(f"ERROR: {e}")
        
        time.sleep(1)
    
    return {"status": "FAILED", "message": "Max retries reached"}

if __name__ == "__main__":
    print("--- RPC CLIENT START ---")
    
    result = rpc_call("add", {"a": 5, "b": 7})
    
    print("\nFinal Response from Server:")
    print(json.dumps(result, indent=4))
    print("--- RPC CLIENT END ---")