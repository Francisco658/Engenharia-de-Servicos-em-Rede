import math
from socket import socket
import socket

# Funções usadas para a comunicação UDP e TCP entre os componentes da rede

def send_request_resp(HOST, PORT, msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(msg.encode())
        data = s.recv(1024)
        return data.decode()
    return None

def send_requestUDP(HOST, PORT, packet):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect((HOST, PORT))
        s.sendall(packet)

def send_request(HOST, PORT, msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(msg.encode())

# Função usada para calcular a ordem de magnitude das métricas
def orderOfMagnitude(number):
    return math.floor(math.log(number, 10))