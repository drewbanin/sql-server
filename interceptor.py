#!/usr/bin/env python

from gevent import socket, server, Greenlet, joinall
from query import Query
import struct

class Interceptor:

    def query_pipe(self, source_socket, destination_socket):
        while True:
            try:
                data = source_socket.recv(1024)
            except socket.error, e:
                break
            else:
                if data :
                    if data[0] == 'Q':
                        query = Query(data[5:])
                    destination_socket.send(data)
                else:
                    break


    def response_pipe(self, source_socket, destination_socket):
        while True:
            try:
                data = source_socket.recv(1024)
            except socket.error, e:
                break
            if data[0] == 'T':
                length = data[1:5]
                #fields = data[6:8]
                #print repr(data)
                #print "LEN:", length, "FIELDS: ", fields, "LEN2: ", len(data)
                import ipdb; ipdb.set_trace()
            destination_socket.send(data)

    def pg_proxy(self, client_socket, address):
        pg_socket = socket.create_connection(("localhost", 5439))
        pg_socket.settimeout(300.0)
        client_socket.settimeout(300.0)
        joinall((
            Greenlet.spawn(self.query_pipe, client_socket, pg_socket),
            Greenlet.spawn(self.response_pipe, pg_socket, client_socket)
        ))
        pg_socket.close()
        client_socket.close()

    def run(self):
        s = server.StreamServer(("localhost", 9991), self.pg_proxy)
        s.serve_forever()
