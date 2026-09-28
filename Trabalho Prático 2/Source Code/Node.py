import socket
from _thread import *
import time
from utils import send_request, send_request_resp, orderOfMagnitude, send_requestUDP
import time
import socket
from _thread import *
from flooding_utils import *
from time import sleep
from routing_utils import create_descending_packet


class Node:
    neighbors = []
    routing_table = []      # [ IP:PORTA, N_saltos, Métrica geral, Backup, tempoAtéRP ]
    streaming_ips = []      # ips de quem quer a stream
    stream_flowing = False  # indica se a stream está ou não a passar pelo nodo
    still_alive = {}        # 'IP:PORT': Numero de falhas
    receiving_from = ""     # IP de quem esta a receber
    MAX_SA_FAULTS = 4       # Máximo de falhas de SA até ser retirado da routing table
    play = []               # ips a quem deve transmitir a stream
    saR = 0                 # n de pacotes SA recebidos
    initT = time.time()     # timestamp de inicio

    def send_udp(self, packet):
        """
            Quando o nodo recebe um pacote UDP, reencaminha-o para todos os presentes no streaming_ips
        """
        self.stream_flowing=True
        if self.play :
            print(f'[Stream] A enviar a stream para: {self.play}')
            for ip in self.play:
                ipFinal,_ = ip.split(':')
                send_requestUDP(ipFinal,8888,packet)
        self.stream_flowing =False

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


    def initNode(self):
        """
            Start do Node, o still alive e o listenUDP
        """
        res = send_request_resp(self.HOST_B, self.PORT_B, f'C {self.HOST} {self.PORT}')
        self.neighbors = res.split(' ')
        start_new_thread(self.sendStillAlivePacket, ())
        start_new_thread(self.listen_udp, ())

    def handle_unique_way(self):
        while True:
            sleep(5)

            if len(self.routing_table) == 1 and self.routing_table[0][-2] == 0 and int(self.routing_table[0][1]) > 1: # and not be a backup
                packet = f'F UN {self.HOST}:{self.PORT}'
                
                node_ip, node_port = self.routing_table[0][0].split(':')
                print(f'[Flooding] Pacote de pedido Unique enviado para {node_ip}:{node_port}')
                send_request(node_ip, int(node_port), packet)

    def startFlood(self):
        """
            Função que executa o flood de 10 em 10 segundos. Só para de chamar a função sendFloodPacket quando já não há mais nenhum vizinho para o qual fazer flood.
        """
        while True:
            direct_neighbors_routing_table = list(filter(lambda x: x[3] == 0, self.routing_table))
            if len(self.routing_table) < len(self.neighbors):
                sendFloodPacket(direct_neighbors_routing_table, self.neighbors, ':'.join([self.HOST, str(self.PORT)])) #HOST:PORT
            else:
                print('[Flooding] Done')  
                break
            time.sleep(10)

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

    def sendStillAlivePacket(self):
        """
            Função que envia Still Alives de 20 em 20 segundos
        """
        while(True):
            self.handle_routing_table_sa()
            self.handle_streaming_ips_sa()
            print("[ROUTING TABLE] ", self.routing_table)
            time.sleep(20)

    def handle_routing_table_sa(self):
        """
            Faz a verificação para ver se nenhum dos presentes na routing_table já atingiu o máximo de falhas aos SA.
            Envia pedidos de SA para os que não tiverem atingido
        """

        for ip in self.routing_table:
            if self.still_alive[ip[0]] and self.still_alive[ip[0]] == self.MAX_SA_FAULTS:
                self.routing_table.remove(ip)
                del self.still_alive[ip[0]]

                if self.stream_flowing == True and self.receiving_from == ip[0]:
                    ascending_packet = f'STR C 1 {self.HOST}:{self.PORT}'
                    host, port = self.routing_table[0][0].split(':')
                    self.receiving_from = ''
                    self.stream_flowing = False
                    send_request(host, int(port), ascending_packet)

                continue

            self.send_sa_request(ip[0])

    def handle_streaming_ips_sa(self):
        """
            Faz a verificação para ver se nenhum dos presentes nos streaming_ips já atingiu o máximo de falhas aos SA.
            Se o tiverem feito, retira-os da lista, caso contrário envia pedido de SA.
        """
        for ip in self.streaming_ips:
            if self.still_alive[ip] == self.MAX_SA_FAULTS:
                self.streaming_ips = list(filter(lambda a: a != ip, self.streaming_ips))
                del self.still_alive[ip]
                continue

            self.send_sa_request(ip)

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
        elif packet[0] == 'SA':
            self._treat_stillAlive_packet(packet)
        elif packet[0] == 'STR':
            self._treat_TCP_streaming_packet(packet)
        elif packet[0] == 'P':
            self._treat_streaming_RS_packet(packet)
     
    def _treat_stillAlive_packet(self, packet):
        """
            Handler do pacote de Still Alive
        """
        ips = [x[0] for x in self.routing_table]
        self.saR = self.saR + 1
        t = time.time()
        packetloss = (((t-self.initT)*len(self.routing_table))/10) - self.saR

        if packet[1] in ips:

            latency = float(packet[3]) + (t - float(packet[2])) + packetloss

            for i,ip in enumerate(self.routing_table):

                addr, n_jumps, _, backup, _ = ip
                if ip[0] != packet[1]: continue

                new_node = (addr, n_jumps, orderOfMagnitude(latency), backup, latency)
                self.add_node_to_routing_table(new_node)

                if self.receiving_from.strip() != '' and self.receiving_from != self.routing_table[0][0]:
                    self.countdown -= 1
                else:
                    self.countdown = 3


                # O countdown serve para verificar se a mudança na ordem da routing_table não foi apenas algum pontual
                if self.receiving_from.strip() != '' and self.receiving_from != self.routing_table[0][0] and self.countdown == 0:
                    
                    # Pedido para terminar stream vinda do receiving_from
                    host, port = self.receiving_from.split(':')
                    stop_packet = f'STR S {self.HOST}:{self.PORT}'
                    self.receiving_from = ''
                    self.stream_flowing = False
                    send_request(host, int(port), stop_packet)

                    # Pedido para começar fluxo de stream enviada por self.routing_table[0][0]
                    ascending_packet = f'STR C 1 {self.HOST}:{self.PORT}'
                    host, port = self.routing_table[0][0].split(':')
                    send_request(host, int(port), ascending_packet)

                    pass

                break

        self.still_alive[packet[1]] = 0

    def _treat_TCP_streaming_packet(self, packet):
        """
            Handler dos pacotes de estabelecimento de stream
        """

        #Stream request
        if packet[1] == 'C': 
            self._handle_ascending_stream_packet(packet)
        #Stream request response
        elif packet[1] == 'D':
            self._handle_descending_stream_packet(packet)
        #Stream stop request
        elif packet[1] == 'S':
            self._handle_stop_stream_packet(packet)

    def _handle_ascending_stream_packet(self, packet):
        """
            Handler dos pacotes de stream "STR C", ou seja, ascendentes
        """
        print(f'[STR C] Recebido pacote: {packet}')
        if not self.stream_flowing:
            self.handle_stream_not_flowing(packet)
        else:
            self._send_stream_descending_packet(packet)

    def handle_stream_not_flowing(self, packet):
        """
            Handler para quando o nodo não tem sinal em si
        """
        start_new_thread(self.startFlood, ()) # Flood é iniciado
        time.sleep(3)
        numeroIps = int(packet[2])
        packet[2] = str(numeroIps+1)
        packet.append(f'{self.HOST}:{self.PORT}')
        ipsSent = packet[3:3+numeroIps]
        sent = False

        #Mandamos para o melhor se não tiver passado lá ainda.
        if self.routing_table:
            host, port = self.routing_table[0][0].split(':')
            send_request(host, int(port), ' '.join(packet))
            sent = True

        self.play.append(packet[3])

    def _send_stream_descending_packet(self, packet):
        """
            Handler para quando o nodo tem sinal em si. 
        """
        self.play.append(packet[3])
    

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

        if packet[2] in self.play:
            self.play.remove(packet[2])

        if len(self.play) == 0 and len(self.streaming_ips)==0 and self.routing_table:
            host, port = self.routing_table[0][0].split(':')
            send=' '.join([str(elem) for elem in packet])
            send_request_resp(host, int(port), send)
            self.stream_flowing=False

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
            _, _, _, backup_node_ip, n_nodes, time_to_server = flood_packet
            new_node = (backup_node_ip, int(n_nodes), float(time_to_server), 1, float(time_to_server))
            self.add_node_to_routing_table(new_node)
            self.still_alive[backup_node_ip] = 0
            print(f'[Flooding] Registado nodo de backup {backup_node_ip} na routing_table')
        elif self.routing_table: # Pedido
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
        if flood_packet[1] == 'UN':
            self._handle_unique_way_packet(flood_packet)
        elif flood_packet[1] == 'R':
            self._handle_flood_response_packet(flood_packet)
        elif flood_packet[1] == 'X':
            self._handle_flood_response_packetX(flood_packet)
        elif flood_packet[1] == 'S':
            self._handle_flood_response_packet_server(flood_packet)
        else:
            self._send_flood_response(flood_packet[1])

    def _send_flood_response(self, origin):
        """
            Envia resposta de flood, com o ip próprio, o n_saltos do nodo seguinte, a latencia do RP até si próprio e um timestmap
        """

        print(f'[Flooding] A enviar um pacote de resposta a um pedido de flood vindo de {origin}')
        host, port = origin.split(':')
        if len(self.routing_table)>0:
            pacote = f'F R {self.HOST}:{self.PORT} {int(self.routing_table[0][1])+1} {self.routing_table[0][4]} {time.time()}'
            send_request(host, int(port), pacote)
        else:
            pacote = f'F R {self.HOST}:{self.PORT} 1 0 {time.time()}'
            send_request(host, int(port), pacote)

    def _handle_flood_response_packet_server(self, origin):
        """
            Envia resposta de flood, com o ip próprio, o n_saltos do nodo seguinte, a latencia do RP até si próprio e um timestmap
        """

        print(f'[Flooding] A enviar um pacote de resposta a um pedido de flood vindo de {origin[2]}')
        start_new_thread(self.startFlood, ()) ##
        time.sleep(3)
        if self.routing_table:
            listV = origin[3].split(' ')
            host, port = origin[2].split(':')
            result_string = ', '.join(listV)
            pacote = f'F S {self.HOST}:{self.PORT} {result_string} {int(self.routing_table[0][1])+1} {self.routing_table[0][4]} {time.time()} {host}:{port}'

            for neighbor in self.neighbors:
                if neighbor:
                    #Enviar pacoteEnvio
                    host, port = neighbor.split(':')
                    try:
                        send_request(host, int(port), pacote)
                    except ConnectionRefusedError:
                        print(f'{host}:{port} is down')

    
    def _handle_flood_response_packetX(self,origin):
        """
            Envia resposta de flood, com o ip próprio, o n_saltos do nodo seguinte, a latencia do RP até si próprio e um timestmap
        """

        print(f'[Flooding] A enviar um pacote de resposta a um pedido de flood vindo de {origin[2]}')
        if self.routing_table:
            listV = origin[3].split(' ')
            host, port = origin[2].split(':')
            result_string = ', '.join(listV)
            pacote = f'F X {host}:{port} {int(self.routing_table[0][1])+1} {self.routing_table[0][4]} {time.time()}'

            for neighbor in self.neighbors:
                if neighbor:
                    #Enviar pacoteEnvio
                    host, port = neighbor.split(':')
                    try:
                        send_request(host, int(port), pacote)
                    except ConnectionRefusedError:
                        print(f'{host}:{port} is down')

    
    def _treat_streaming_RS_packet(self,packet):
        print(f'[STREAM] A enviar um pacote de pedido de stream para servidor {packet[1]}')
        send=' '.join([str(elem) for elem in packet])
        for neighbor in self.neighbors:
            if neighbor:
                #Enviar pacoteEnvio
                host, port = neighbor.split(':')
                try:
                    send_request(host, int(port), send)
                except ConnectionRefusedError:
                    print(f'{host}:{port} is down')
