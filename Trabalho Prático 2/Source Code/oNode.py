import sys
from Node import Node
import threading

def main():
    HOST = sys.argv[1]          # IP do Nodo
    PORT = int(sys.argv[2])     # Porta do Cliente
    HOST_B = sys.argv[3]        # IP do BS
    PORT_B = int(sys.argv[4])   # Porta do BS
    o_node = Node(HOST, PORT, 2800, HOST_B, PORT_B)
    o_node.initNode()
    handle_unique_way_thread = threading.Thread(target=o_node.handle_unique_way, daemon=True)
    handle_unique_way_thread.start()
    print("Boostrap:", o_node.neighbors)
    o_node.listen()

    handle_unique_way_thread.join()

    return

if __name__ == '__main__':
    main()