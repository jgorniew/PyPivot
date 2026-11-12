import socket

HOST = "172.29.20.10"
PORT = 9900

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"[TARGET] Listening on {HOST}:{PORT}")

    communication_socket, address = server.accept()

    with communication_socket:
        print(f"[TARGET] Connected: {address}")

        message = communication_socket.recv(4096)
        print(f"[TARGET] Received: {message}")

        communication_socket.sendall(b"Got the message!")
        print("[TARGET] Response sent")