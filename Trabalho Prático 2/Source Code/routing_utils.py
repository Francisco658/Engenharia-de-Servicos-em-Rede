#Cria pacote descendente que vai do Rendezvous Point para o Client pela rota mais rápida previamente calculada
def create_descending_packet(climbing_packet, ip_node):
    newPacket = ['STR','D']

    numeroIps = int(climbing_packet[2])
    ipsSent = climbing_packet[3:3+numeroIps]

    for i,ip in enumerate(ipsSent):
        if i+1 < len(ipsSent):
            try:
                index = ipsSent.index(ip, i+1)
                del ipsSent[i:index]
            except ValueError:
                continue

    newPacket.append(str(len(ipsSent) + 1))
    newPacket += ipsSent
    newPacket.append(ip_node)

    return newPacket