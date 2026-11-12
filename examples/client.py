import socket

HOST = "172.29.10.20"
PORT = 8888

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect((HOST, PORT))

    print("[OPERATOR] Connected to Pivot")

    client.sendall(b"Hello")
    print("[OPERATOR] Message sent")

    response = client.recv(4096)
    print(f"[OPERATOR] Response: {response}")