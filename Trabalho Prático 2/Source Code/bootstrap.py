import socket
from _thread import *

class Bootstrap:
    ThreadCount = 0
    dictNeighbors = {}

    def __init__(self, host, port):
        self.host = host
        self.port = port

    #Lê o ficheiro de texto com a informação da topologia
    def readNeighbors(self, filename):
        with open(filename,'r') as f:
            lines = f.readlines()
            print(lines, len(lines))
            for i in range(0, len(lines), 2):
                print(i)
                ipNative = lines[i].rstrip('\n')
                ipNeighbors = lines[i+1].rstrip('\n')
                self.dictNeighbors[ipNative] = [tuple((addr).split(':')) for addr in ipNeighbors.split(' ')]

        print(self.dictNeighbors)
    
    def send(self, client, resp):
        start_new_thread(self._multi_threaded_client, (client, resp))
        self.ThreadCount += 1

    #Responde aos pedidos de conhecimentos dos vizinhos de todos os componentes da rede
    def handle_packet(self, packet, client):
        if packet[0] == 'C':
            _, client_host, client_port = packet.split(' ')
            print("REC", client_host, client_port)
            client_label = ':'.join((client_host, client_port))
            client_neighbors = self.dictNeighbors[client_label]
            client_n_t =  ' '.join([':'.join(addr) for addr in client_neighbors])
            print("=>", client_n_t)
            resp = str.encode(client_n_t)
            print(resp)
            self.send(client, resp)

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ServerSideSocket:
            ServerSideSocket.bind((self.host,self.port))

            print('Socket is listening...')

            ServerSideSocket.listen(1)

            while True:
                try:
                    Client, address = ServerSideSocket.accept()
                    print('Connected to: ' + address[0] + ':' + str(address[1]))
                    req = Client.recv(1024).decode()
                    self.handle_packet(req, Client)
                    
                    print('Thread Number: ' + str(self.ThreadCount))
                except KeyboardInterrupt:
                    break
                
            ServerSideSocket.close()

    def _multi_threaded_client(self, connection, msg):
        try:
            connection.sendall(msg)
        except socket.error as e:
            print(str(e))

        connection.close()
