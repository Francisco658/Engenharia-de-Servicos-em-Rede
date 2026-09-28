import sys
import time
import socket
import cv2
import imutils
import base64
import numpy as np
from time import sleep
from _thread import *
from utils import send_request, send_request_resp, orderOfMagnitude, send_requestUDP
from routing_utils import create_descending_packet
from flooding_utils import *

filenames = ["video.mp4"]
BUFF_SIZE = 65536
WIDTH = 400

class Server:
    neighbors = []
    streaming_ips = []  # ip do RP
    still_alive = {}    # 'IP:PORT': Numero de falhas
    routing_table = []  # [ IP:PORTA, N_saltos, OG tempo até RP, Backup, tempoAtéServidor_clean ]
    flag = True

    def send_udp(self,video):
        """
            Função que envia os pacotes do vídeo
        """
        pass
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as server_socket:
           server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,BUFF_SIZE)
           server_socket.bind((self.HOST,8888))

           vid = cv2.VideoCapture(video)
           FPS = vid.get(cv2.CAP_PROP_FPS)
           TS = (1/FPS)
           i = 0
           
           while True:
               
                while len(self.routing_table) == 0:
                   time.sleep(1)
                # Executa o vídeo, get o frame
                ret,frame = vid.read()

                if not ret:
                   # Se sim, volte ao início do vídeo
                   vid.set(cv2.CAP_PROP_POS_FRAMES, 0)
                   continue
                
                frame = imutils.resize(frame,width=WIDTH)
                _, buffer = cv2.imencode('.jpeg',frame,[cv2.IMWRITE_JPEG_QUALITY,80])
                data_encode = np.array(buffer)

                #Transforma o array numpy em um array de bytes
                message = data_encode.tobytes()

                #print(f'Enviei pacote UDP {i}')
                i += 1
                
                ipFinal,_ = self.routing_table[0][0].split(':') 
                server_socket.sendto(message,(ipFinal,8888))
                cv2.waitKey(int(450*TS))

                if self.flag==False : break

    def startFlood(self):
        """
            Função que executa o flood de 10 em 10 segundos. Só para de chamar a função sendFloodPacket quando já não há mais nenhum vizinho para o qual fazer flood.
        """
        while True:
            direct_neighbors_routing_table = list(filter(lambda x: x[3] == 0, self.routing_table))
            if len(direct_neighbors_routing_table) < len(self.neighbors):
                announcePacketServer(self.HOST_RP,self.PORT_RP,filenames, ':'.join([self.HOST, str(self.PORT)])) #HOST:PORT
                time.sleep(10)
        
    def __init__(self, HOST, PORT, HOST_B, PORT_B, HOST_RP, PORT_RP):
        self.HOST = HOST
        self.PORT = PORT
        self.HOST_B = HOST_B
        self.PORT_B = PORT_B
        self.HOST_RP = HOST_RP
        self.PORT_RP = PORT_RP

    def initServer(self):
        res = send_request_resp(self.HOST_B, self.PORT_B, f'C {self.HOST} {self.PORT}')
        self.neighbors = res.split(' ')
        start_new_thread(self.startFlood, ())

    def handle_packet(self,packet):
        """
            Packet Handler
        """
        packet = packet.split(' ')
        if packet[0] == 'F':
            self._treat_flood_packet(packet)

        elif packet[0] == 'SA':

            host, port = packet[1].split(':')
            print(f'[Flooding] Recebido de {host}')
            send_request(host, int(port), f'SA {self.HOST}:{self.PORT} {time.time()} 0')
        
        elif packet[0] == 'P':
            #se for o seu ip e tiver o video começa a mandar a stream pedida 
            host, port = packet[1].split(':')
            video = packet[2]
            
            if host==self.HOST and video in filenames:
                print(f'[STREAM P] Recebido pedido de stream')
                self.send_udp(video)
                self.flag = True

        elif packet[0] == 'STR':
            if packet[1] == 'C':
            
                newPacket = create_descending_packet(packet,f'{self.HOST}:{self.PORT}')

                self.streaming_ips.append(newPacket[len(newPacket) - 2])
                print("Atualizada streaming_ips: " + str(self.streaming_ips))
                host, port = newPacket[len(newPacket) - 2].split(':')
                print(f'[STREAM C] Recebido de {host}')
                send_request(host,int(port),' '.join(newPacket))
                print(f'[STREAM D] Enviado para {host}')

            elif packet[1] == 'S': 
                try:
                    self.flag=False
                    self.streaming_ips = list(filter(lambda a: a != packet[2], self.streaming_ips))
                    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as server_socket:
                        server_socket.close()
                    print(f'[STREAM S] Recebido de {packet[2]}')
                except Exception:
                    print(Exception)
        return

    def _treat_flood_packet(self, flood_packet):
        """
            Handler geral dos pacotes de Flood (prefixo 'F')
            Não respondemos aos pedidos de Flood quando a nossa routing_table está vazia.
        """

        #F IP NmrNodos TS
        if flood_packet[1] == 'UN':
            self._handle_unique_way_packet(flood_packet)

        if flood_packet[1] == 'X':
            self._handle_flood_response_packet(flood_packet)


    def _handle_flood_response_packet(self, flood_packet):
        """
            Handler dos pacotes F X, de resposta ao pedido de annoucement
        """
        
        _, _, node_ip, n_nodes, time_to_server, timestamp = flood_packet
        total_time = float(time_to_server) + (time.time() - float(timestamp))
        print(f'[Annoucement] Resposta registada na routing table {node_ip} {n_nodes} {total_time}')
        new_node = (node_ip, int(n_nodes), orderOfMagnitude(total_time), 0, total_time)
        self.add_node_to_routing_table(new_node)

        self.still_alive[node_ip] = 0

    def _handle_unique_way_packet(self, flood_packet):
        """
            Handler dos pacotes F UN
            Se for F UN R, registamos os dados enviamos na routing_table
        """
        if flood_packet[2] == 'R': # Is a response
            _, _, _, backup_node_ip, n_nodes, time_to_server = flood_packet
            new_node = (backup_node_ip, int(n_nodes), float(time_to_server), 1, float(time_to_server))
            self.add_node_to_routing_table(new_node)
            self.still_alive[backup_node_ip] = 0
            print(f'[Flooding] Registado nodo de backup {backup_node_ip} na routing_table')
        else: # Is a request
            _, _, origin = flood_packet
            origin_host, origin_port = origin.split(':')
            sendPacket = f'F UN R {self.routing_table[0][0]} {self.routing_table[0][1]} {self.routing_table[0][2]}'
            send_request(origin_host, int(origin_port), sendPacket)
            print(f'[Flooding] Enviado o nosso nodo fornecedor para {origin}')

    def _send_flood_response(self, origin):
        """
            Envia resposta de flood, com o ip próprio, o n_saltos do nodo seguinte, a latencia do RP até si próprio e um timestmap
        """

        print(f'[Flooding] A enviar um pacote de resposta a um pedido de flood vindo de {origin}')
        host, port = origin.split(':')
        pacote = f'F R {self.HOST}:{self.PORT} {int(self.routing_table[0][1])+1} {self.routing_table[0][4]} {time.time()}'
        send_request(host, int(port), pacote)
 
    def add_node_to_routing_table(self, new_node):
        """
            Adiciona um nodo à routing table
            O nodo tem de ter a seguinte arquitetura: (<IP>,<N_SALTOS>,<OG(LATENCIA)>, <BOOL_BACKUP>, <LATENCIA>)
        """
        tmp_routing_table = list(filter(lambda x: x[0] != new_node[0], self.routing_table))
        tmp_routing_table.append(new_node)
        self.routing_table = sorted(tmp_routing_table, key=lambda x: [x[3], x[2], x[1]])

    def listen(self):
        """
            TCP Listener
        """
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ServerSideSocket:
            ServerSideSocket.bind((self.HOST, self.PORT))

            print('Server is listening on', self.HOST, self.PORT)

            ServerSideSocket.listen(1)

            while True:
                try:
                    Client, address = ServerSideSocket.accept()
                    packet = Client.recv(1024).decode()
                    start_new_thread(self.handle_packet,(packet,))

                    Client.close()
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print("Exception", e)
                    break
                
            ServerSideSocket.close()

    def handle_unique_way(self):
        while True:
            sleep(5)
            #print(f'[Flooding] Unique check: {len(self.routing_table) == 1 and self.routing_table[0][-2] == 0 and int(self.routing_table[0][1]) > 1}')
            if len(self.routing_table) == 1 and self.routing_table[0][-2] == 0 and int(self.routing_table[0][1]) > 1: # and not be a backup
                packet = f'F UN {self.HOST}:{self.PORT}'
                
                node_ip, node_port = self.routing_table[0][0].split(':')
                print(f'[Flooding] Pacote de pedido Unique enviado para {node_ip}:{node_port}')
                send_request(node_ip, int(node_port), packet)