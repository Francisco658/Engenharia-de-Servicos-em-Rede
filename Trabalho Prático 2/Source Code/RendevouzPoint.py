import socket
from _thread import *
import time
# from Client import Client
from utils import send_request, send_request_resp, orderOfMagnitude, send_requestUDP
import time
import socket
from _thread import * #Trocar para threading
from flooding_utils import *
from time import sleep
#from threading import
from routing_utils import create_descending_packet


class RendevouzPoint:
    neighbors = []
    routing_table = {}      # 'F S {self.HOST}:{self.PORT} {result_string} {int(self.routing_table[0][1])+1} {self.routing_table[0][4]} {time.time() ip_server}'
    streaming_ips = {}      # ips de quem quer a stream
    stream_flowing = False  # indica se a stream está ou não a passar pelo RP
    still_alive = {}        # 'IP:PORT': Numero de falhas
    receiving_from = ""     # IP de quem esta a receber
    MAX_SA_FAULTS = 4       # Máximo de falhas de SA até ser retirado da routing table

    def send_udp(self, packet):
        """
            Quando o nodo recebe um pacote UDP, reencaminha-o para todos os presentes no streaming_ips
        """
        self.stream_flowing=True
        for ip in self.streaming_ips.values():
            ipFinal,_ = ip[5].split(':')
            send_requestUDP(ipFinal,8888,packet)
        self.stream_flowing=False

    def listen_udp(self):
        """
            Listener de pacotes UDP. Quando um pacote chega, chama a função send_udp que o vai reencaminhar para todos os streaming_ips
        """

        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as ServerSideSocket:
            ServerSideSocket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,self.BUFF_SIZE)
            ServerSideSocket.bind((self.HOST, 8888))

            while True:

                while(len(self.routing_table) == 0):
                    sleep(0.1)

                try:
                    packet, _ = ServerSideSocket.recvfrom(self.BUFF_SIZE)
                    #print('=> Recebi pacote UDP')
                    start_new_thread(self.send_udp,(packet,))

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print("Exception:", e)
                    break
                    
            ServerSideSocket.close()

    def __init__(self, HOST, PORT, PORT_UDP, HOST_B, PORT_B):
        self.HOST = HOST
        self.PORT = PORT
        self.PORT_UDP = PORT_UDP
        self.HOST_B = HOST_B
        self.PORT_B = PORT_B
        self.BUFF_SIZE = 65532
        self.countdown = 3


    def initRP(self):
        """
            Start do Node, inicia o flood, o still alive e o listenUDP
        """
        res = send_request_resp(self.HOST_B, self.PORT_B, f'C {self.HOST} {self.PORT}')
        self.neighbors = res.split(' ')
        start_new_thread(self.listen_udp, ())

    def listen(self):
        """
            Listener TCP básico que cria uma thread quando é estabelecida uma conexão.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ServerSideSocket:
            ServerSideSocket.bind((self.HOST, self.PORT))
            ServerSideSocket.listen(1)

            while True:
                try:
                    Client, address = ServerSideSocket.accept()
                    packet = Client.recv(1024).decode()
                    packet = packet.split(' ')
                    if packet[-1] == '': packet = packet[:-1]
                    start_new_thread(self._handle_packet,(packet,))
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    break
                    
            ServerSideSocket.close()

    def send_sa_request(self, ip):
        """
            Envia request SA para o ip dado
        """

        host, port = ip.split(':')
        self.still_alive[ip] += 1

        try:  
            send_request(host, int(port), f'SA {self.HOST}:{self.PORT} {time.time()} {self.routing_table[0][4]}')
            print(f'[SA] Enviado para {host}:{port}')
        except:
            return

    def _handle_packet(self, packet):
        """
            Packet Handler
        """
        if packet[0] == 'F':
            self._treat_flood_packet(packet)
        elif packet[0] == 'SA': pass
        elif packet[0] == 'STR':
            self._treat_TCP_streaming_packet(packet)
        elif packet[0] == 'P': pass

    def _treat_TCP_streaming_packet(self, packet):
        """
            Handler dos pacotes de estabelecimento de stream
        """

        if packet[1] == 'C':
            self._handle_ascending_stream_packet(packet)
        elif packet[1] == 'D':
            self._handle_descending_stream_packet(packet)
        elif packet[1] == 'S':
            self._handle_stop_stream_packet(packet)

    def _handle_ascending_stream_packet(self, packet):
        """
            Handler dos pacotes de stream "STR C", ou seja, ascendentes
        """
        print(f'[STR C] Cliente {packet[3]} pediu video: {packet[4]}')
        print(self.stream_flowing)
        if self.stream_flowing==False:
            #se tiver a stream manda ao melhor servidor, senao devolve a quem recebeu
            streams = []
            #retira o nome do video do pacote
            video = packet[4]
            print(video)

            print(f'[RP] Verificando servidores disponiveis com o video: {video}')
            for x in self.routing_table.values():
                #adicionar tds os que contêm o video
                if video in x: streams.append(x)
                print(x)
            
            print(streams)

            if streams :
                print(f'[RP] Calculando melhor servidor com o video: {video}')

                best=streams[0]
                if len(best.split(','))>2:
                    bestTime = best.split(',')[4]
                    for x in streams:
                        time = x.split(',')[4]
                        if time<bestTime: best=x

                    srv = best.split(',')[5]
                    print(f'[RP] Melhor servidor: {srv}')

                    host, port = srv.split(':')
                    sendPacket = f'P {host}:{port} {video}'
                else:
                    srv = best.split(',')[0]

                flag = True
                #enviar para o servidor o pedido se nao tiver já pedido
                for key in self.streaming_ips.keys():
                    if packet[3] in key : flag= False

                if flag:
                    self.streaming_ips[packet[3]]=packet
                    print(f'[RP] Lista de Streams pedidas atualizada: {self.streaming_ips.keys()}')
                    host, port = srv.split(':')
                    sendPacket = f'P {host}:{port} {video}'
                    origin_host, origin_port = best.split(',')[0].split(':')
                    send_request_resp(origin_host, int(origin_port), sendPacket)
        else:
            self.streaming_ips[packet[3]]=packet
            print(f'[RP] Lista de Streams requisitadas atualizada: {self.streaming_ips.keys()}')


    def handle_stream_not_flowing(self, packet):
        """
            Handler para quando o nodo não tem sinal em si
        """
        numeroIps = int(packet[2])
        packet[2] = str(numeroIps+1)
        packet.append(f'{self.HOST}:{self.PORT}')
        ipsSent = packet[3:3+numeroIps]
        sent = False

        #Mandamos para o melhor se não tiver passado lá ainda.
        for route in self.routing_table:
            if route[0] not in ipsSent:
                host, port = route[0].split(':')
                send_request(host, int(port), ' '.join(packet))
                sent = True
                break
        
        #Se não houver nenhum nodo por onde possa passar, isto é, onde o pacote já não tenha estado, devolvemos a quem nos enviou.
        if not sent:
            host, port = packet[2+numeroIps]
            send_request(host, int(port), ' '.join(packet))

    def _send_stream_descending_packet(self, packet):
        """
            Handler para quando o nodo tem sinal em si. 
            Quando isso acontece, criamos um pacote 'STR D' e enviamos para baixo, pois não precisamos de ir até ao servidor.
        """

        newPacket = create_descending_packet(packet,f'{self.HOST}:{self.PORT}')
        self.still_alive[newPacket[len(newPacket) - 2]] = 0
        host, port = newPacket[len(newPacket) - 2].split(':')
        send_request(host, int(port), ' '.join(newPacket))
        print(f'[STR D] Enviado para {host}')

    
    def _handle_descending_stream_packet(self, packet):
        """
            Handler para pacotes 'STR D'
        """

        n_ips = int(packet[2])
        packet[2] = str(n_ips-1)
        self.stream_flowing = True
        self.receiving_from = packet[len(packet) - 1]
        new_packet = packet[:-1]
        new_packet_addr = new_packet[len(new_packet) - 2]
        self.countdown = 3
        
        #Só enviamos para baixo se n_ips ainda for maior que 2
        if n_ips > 2:
            self.streaming_ips.append(new_packet_addr)
            self.still_alive[new_packet_addr] = 0
            host, port = new_packet_addr.split(':')
            send_request(host, int(port), ' '.join(new_packet))

    def _handle_stop_stream_packet(self, packet):
        """
            Handler para pacotes "STR S"
            Eliminamos o ip de onde vem o pacote da nossa lista de streaming_ips
            Se a nossa lista ficar vazia, significa que já não precisamos de fluxo, pelo que transmitivos isso ao receiving_from, para que nos pare de enviar.
        """
        self.streaming_ips.pop(packet[2],None)
        self.stream_flowing=False
        for key,value in self.routing_table.items():
            #mandar para o server
            ad = value.split(', ')[0]
            host,port = ad.split(':')
            stop_packet = f'STR S {self.HOST}:{self.PORT}'
            send_request(host, int(port), stop_packet)
            

    def add_node_to_routing_table(self, new_node):
        """
            Adiciona um nodo à routing table
            O nodo tem de ter a seguinte arquitetura: (<IP>,<N_SALTOS>,<OG(LATENCIA)>, <BOOL_BACKUP>, <LATENCIA>)
        """
        tmp_routing_table = list(filter(lambda x: x[0] != new_node[0], self.routing_table))
        tmp_routing_table.append(new_node)
        self.routing_table = sorted(tmp_routing_table, key=lambda x: [x[3], x[2], x[1]])

    def _handle_unique_way_packet(self, flood_packet):
        """
            Handler dos pacotes F UN
            Se for F UN R, registamos os dados enviamos na routing_table
            Se for F UN, enviamos os dados do nosso fornecedor
        """
        if flood_packet[2] == 'R': # Resposta
           pass
        else: # Pedido
            _, _, origin = flood_packet
            origin_host, origin_port = origin.split(':')
            sendPacket = f'F UN R {self.routing_table[0][0]} {self.routing_table[0][1]} {self.routing_table[0][2]}'
            send_request(origin_host, int(origin_port), sendPacket)
            print(f'[Flooding] Enviado o nosso nodo fornecedor para {origin}')

    def _handle_flood_response_packet(self, flood_packet):
        """
            Handler dos pacotes F R, de resposta ao pedido de flood
        """
        
        _, _, node_ip, n_nodes, time_to_server, timestamp = flood_packet
        total_time = float(time_to_server) + (time.time() - float(timestamp))
        print(f'[Flooding] Resposta registada na routing table {node_ip} {n_nodes} {total_time}')
        new_node = (node_ip, int(n_nodes), orderOfMagnitude(total_time), 0, total_time)
        self.add_node_to_routing_table(new_node)

        self.still_alive[node_ip] = 0

    def _treat_flood_packet(self, flood_packet):
        """
            Handler geral dos pacotes de Flood (prefixo 'F')
            Não respondemos aos pedidos de Flood quando a nossa routing_table está vazia.
        """
        if flood_packet[1] == 'S':
            self._send_Annoucement_response_server(flood_packet)
        elif flood_packet[1] == 'UN':
            #RP não responde a pedidos de UN
            pass
        elif flood_packet[1] == 'X': 
            pass
        else:
            self._send_flood_response(flood_packet[1])

    def _send_flood_response(self, origin):
        """
            Envia resposta de flood, com o ip próprio, o n_saltos do nodo seguinte, a latencia do servidor até si próprio e um timestmap
        """
        if origin != 'R':
            origin_host, origin_port = origin.split(':')
            print(f'[Flooding] Recebido de {origin_host}')
            sendPacket = f'F R {self.HOST}:{self.PORT} 1 0 {time.time()}'
            send_request(origin_host, int(origin_port), sendPacket)
    
    def _send_Annoucement_response_server(self,flood_packet):
        #adicionar stream à tabela
        result_string = ', '.join(map(str, flood_packet[2:]))
        self.routing_table[flood_packet[2]]=result_string 
        origin_host, origin_port = flood_packet[2].split(':')
        print(f'[RP] Annoucement response sent to {origin_host}') 
        
        #enviar resposta de flood
        sendPacket = f'F X {self.HOST}:{self.PORT} 1 0 {time.time()}'
        send_request(origin_host, int(origin_port), sendPacket)