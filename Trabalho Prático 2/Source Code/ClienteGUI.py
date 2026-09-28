from tkinter import *
from PIL import Image, ImageTk, ImageFile
import socket, threading, sys, traceback, os
import cv2
from utils import send_request
import numpy as np
import random

ImageFile.LOAD_TRUNCATED_IMAGES = True
CACHE_FILE_NAME = "cache-"
CACHE_FILE_EXT = ".jpg"

class ClienteGUI:

	BUFF_SIZE = 65536
	
	# Initiation..
	def __init__(self, master, HOST, PORT, HOST_N, PORT_N, VIDEO):
		self.receiving_from = ""
		self.master = master
		self.createWidgets()
		self.sessionId = random.randint(0,99999999)
		self.HOST = HOST
		self.listen = False
		self.PORT = PORT
		self.HOST_N = HOST_N
		self.PORT_N = PORT_N
		self.still_alive = 0
		self.VIDEO = VIDEO
		
	def createWidgets(self):
		"""Build GUI."""
		# Create Setup button
		self.setup = Button(self.master, width=20, padx=3, pady=3)
		self.setup["text"] = "Setup"
		self.setup["command"] = self.setupMovie
		self.setup.grid(row=1, column=0, padx=2, pady=2)
		
		# Create Play button		
		self.start = Button(self.master, width=20, padx=3, pady=3)
		self.start["text"] = "Começar stream"
		self.start["command"] = self.playMovie
		self.start.grid(row=1, column=1, padx=2, pady=2)
		
		# Create Pause button			
		self.pause = Button(self.master, width=20, padx=3, pady=3)
		self.pause["text"] = "Parar stream"
		self.pause["command"] = self.pauseMovie
		self.pause.grid(row=1, column=2, padx=2, pady=2)
		
		# Create Teardown button
		self.teardown = Button(self.master, width=20, padx=3, pady=3)
		self.teardown["text"] = "Sair"
		self.teardown["command"] =  self.exitClient
		self.teardown.grid(row=1, column=3, padx=2, pady=2)
		
		# Create a label to display the movie
		self.label = Label(self.master, height=19)
		self.label.grid(row=0, column=0, columnspan=4, sticky=W+E+N+S, padx=5, pady=5)

	def setupMovie(self):
		"""Setup button handler."""
		print("Not implemented...") 

	def send_stream_request(self):
		while True:
			try:
				send_request(self.HOST_N, int(self.PORT_N), f'STR C 1 {self.HOST}:{self.PORT} {self.VIDEO}') 
				break
			except:
				continue
            
			time.sleep(10)
	
	def send_stream_stop(self):
		while True:
			try:
				send_request(self.HOST_N, int(self.PORT_N), f'STR S {self.HOST}:{self.PORT}')
				self.listen = False
				print('A terminar')
				break
			except:
				continue
            
			time.sleep(1)

	def exitClient(self):
		"""Teardown button handler."""
		if self.listen:
			self.send_stream_stop()
			self.listen = False
		self.master.destroy() # Close the gui window
		os.remove(CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT) # Delete the cache image from video

	def pauseMovie(self):
		"""Pause button handler."""
		if self.listen:
			self.listen = False
	
	def playMovie(self):
		"""Play button handler."""
		
		if not self.listen:
			self.send_stream_request()
			threading.Thread(target=self.listenRtp).start()
	
	def listenRtp(self):		
		"""Listen for RTP packets."""
		with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as client_socket:
			client_socket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,self.BUFF_SIZE)
			client_socket.bind((self.HOST,8888))
			self.listen = True
			while True:

				if self.listen == False:
					break

				try:
					packet, _ = client_socket.recvfrom(self.BUFF_SIZE)
					print('Recebi pacote UDP')

					frame = cv2.imdecode(np.frombuffer(packet, np.uint8), cv2.IMREAD_COLOR)
						
					self.updateMovie(self.writeFrame(packet))
				except:
					self.send_stream_stop()
					self.client_socket.shutdown(socket.SHUT_RDWR)
					self.client_socket.close()
					break
				
	
	def writeFrame(self, data):
		"""Write the received frame to a temp image file. Return the image file."""
		cachename = CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT
		file = open(cachename, "wb")
		file.write(data)
		file.close()
		
		return cachename
	
	def updateMovie(self, imageFile):
		"""Update the image file as video frame in the GUI."""
		photo = ImageTk.PhotoImage(Image.open(imageFile))
		self.label.configure(image = photo, height=288) 
		self.label.image = photo
		
	def _handle_packet(self, packet):
		if packet[0] == 'STR':
			print(packet)
			self._handle_TCP_streaming_packet(packet)
		elif packet[0] == 'SA':
			self._handle_SA_packet(packet)

	def _handle_TCP_streaming_packet(self, packet):
		if packet[1] == 'D':
			self.receiving_from = packet[2]
			print(f'A receber de {self.receiving_from}')

	def _handle_SA_packet(self, packet):
		self.still_alive = 0
		self.send_still_alive()

	def listenTCP(self):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ServerSideSocket:
			ServerSideSocket.bind((self.HOST, int(self.PORT)))
			print('Socket is listening...')
			ServerSideSocket.listen(1)

			while True:
				try:
					Client, address = ServerSideSocket.accept()
					print('Connected to: ' + address[0] + ':' + str(address[1]))
					packet = Client.recv(1024).decode()
					packet = packet.split(' ')
					if packet[-1] == '': packet = packet[:-1]
					self._handle_packet(packet)

				except KeyboardInterrupt:
					break
				except Exception as e:
					print("Exception:", e)
					break

			ServerSideSocket.close()

	def send_still_alive(self):
		send_request(self.HOST_N, int(self.PORT_N), f'SA {self.HOST}:{self.PORT}')
		self.still_alive += 1