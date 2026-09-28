import sys
from server import Server
import threading

def main():
    HOST = sys.argv[1]          # IP do Server
    PORT = int(sys.argv[2])     # Porta do Server
    HOST_B = sys.argv[3]        # IP do BS
    PORT_B = int(sys.argv[4])   # Porta do BS
    HOST_RP = sys.argv[5]       # IP do RendevouzPoint
    PORT_RP = int(sys.argv[6])  # Porta do RendevouzPoint
    
    o_server = Server(HOST, PORT, HOST_B, PORT_B, HOST_RP, PORT_RP)
    o_server.initServer()

    handle_unique_way_thread = threading.Thread(target=o_server.handle_unique_way, daemon=True)
    handle_unique_way_thread.start()
    print("Boostrap:", o_server.neighbors)
    o_server.listen()

    handle_unique_way_thread.join()

    udpListen_thread = threading.Thread(target=o_server.send_udp, daemon=True)
    udpListen_thread.start()

    o_server.listen()
    udpListen_thread.join()

    return

if __name__ == '__main__':
    main()