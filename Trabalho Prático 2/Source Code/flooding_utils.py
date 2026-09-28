from utils import send_request
import time

#Envia pacotes de flooding para os vizinhos 
def sendFloodPacket(routing_table, neighbors, origin):
    alreadySentIps= [i[0] for i in routing_table]
    packetToSend = f'F {origin} '
    for neighbor in neighbors:
        if neighbor not in alreadySentIps:
            host, port = neighbor.split(':')
            try:
                send_request(host, int(port), packetToSend)
            except ConnectionRefusedError:
                print(f'{host}:{port} is down')

#Anuncia a presença do servidor ao RP
def announcePacketServer(HOST_RP,PORT_RP,filenames,origin):
        packetToSend = f'F S {origin} {filenames}'
        try:
            send_request(HOST_RP, PORT_RP, packetToSend)
        except ConnectionRefusedError:
             print(f'{host}:{port} is down')