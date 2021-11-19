#!/usr/bin/env python3
import sys
import socket
import selectors
import traceback
import libserver

sel = selectors.DefaultSelector()

def accept_wrapper(sock):
	conn, addr = sock.accept() #Accept socket connection - ready to read
	print("Connected:", addr)

	conn.setblocking(False) #Accept data from socket connection

	message = libserver.Message(sel, conn, addr) #Register __init__ function in Message class with socket info

	sel.register(conn, selectors.EVENT_READ, data=message) #Pass mask, socket, and data to selector

if len(sys.argv) != 3:
	print("usage:", sys.argv[0], "<host> <port>")
	sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#Avoid Bind() exception: OSSError: Errno 48] Address already in use
lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

lsock.bind((host, port))
lsock.listen()

print("listening on", (host, port))
lsock.setblocking(False)

sel.register(lsock, selectors.EVENT_READ, data=None)

try:
	while True:
		events = sel.select(timeout=None) #Events ready on the socket

		for key, mask in events:

			if key.data is None:
				accept_wrapper(key.fileobj) #Run accept_wrapper() to register socket with selector for new connection
			else:
				message = key.data #If connection is already established, set sent data to message variable

				try:
					message.process_events(mask) #process_events() | if event = read, call Message.read(), write call Message.write()
				except Exception:
					print( "MAIN: ERROR: Exception for",
						f"{message.addr}:\n{traceback.format_exc()}")
					message.close()

except KeyboardInterrupt:
	print("caught ketboard interrupt, exiting")
finally:
	sel.close()
