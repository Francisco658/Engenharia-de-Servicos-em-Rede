import sys
from bootstrap import Bootstrap

def main():
    HOST = sys.argv[1]  # IP do Bootstrap
    PORT = int(sys.argv[2])  # Porta usada pelo Bootstrap

    bootstrap = Bootstrap(HOST, PORT)
    bootstrap.readNeighbors('bootstrap.txt')
    bootstrap.listen()

    return

if __name__ == '__main__':
    main()