import sys
from RendevouzPoint import RendevouzPoint
import threading

def main():
    HOST = sys.argv[1]          # IP do RendevouzPoint
    PORT = int(sys.argv[2])     # Porta do RendevouzPoint
    HOST_B = sys.argv[3]        # IP do BS
    PORT_B = int(sys.argv[4])   # Porta do BS
    o_rp = RendevouzPoint(HOST, PORT, 2800, HOST_B, PORT_B)
    o_rp.initRP()
    print("Boostrap:", o_rp.neighbors)
    o_rp.listen()

    return

if __name__ == '__main__':
    main()