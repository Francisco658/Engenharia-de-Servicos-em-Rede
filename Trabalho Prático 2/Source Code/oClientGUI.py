import sys
from tkinter import Tk
from ClienteGUI import ClienteGUI
import threading

if __name__ == "__main__":
	try:
		HOST = sys.argv[1] 		# IP do Cliente
		PORT = sys.argv[2] 		# Porta do Cliente
		HOST_N = sys.argv[3] 	# IP do Nodo adjacente
		PORT_N = sys.argv[4]	# Porta do Nodo adjacente
		VIDEO = sys.argv[5]		# Vídeo pedido pelo cliente
	except:
		print("[Usage: ClientLauncher.py Server_name Server_port RTP_port Video_file]\n")	
	
	root = Tk()
	
	# Criar um novo cliente
	app = ClienteGUI(root, HOST, PORT, HOST_N, PORT_N, VIDEO)
	thread_tcp = threading.Thread(target=app.listenTCP, daemon=True)
	thread_tcp.start()
	app.master.title("Client")	
	root.mainloop()
	thread_tcp.join()
	