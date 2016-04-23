#!/usr/bin/env python

from gevent import socket, server, Greenlet, joinall

def pipe(source_socket, destination_socket, modify=False):
    while True:
        try:
            data = source_socket.recv(1024)
        except socket.error, e:
            break
        else:
            if data:
                if modify: data = data.replace("limit 10", "limit 1 ")
                destination_socket.send(data)
            else:
                break

def pg_proxy(client_socket, address):
    pg_socket = socket.create_connection(("localhost", 5439))
    pg_socket.settimeout(300.0)
    client_socket.settimeout(300.0)
    joinall((
        Greenlet.spawn(pipe, client_socket, pg_socket, modify=True),
        Greenlet.spawn(pipe, pg_socket, client_socket, modify=False)
    ))
    pg_socket.close()
    client_socket.close()

if __name__ == '__main__':
    s = server.StreamServer(("localhost", 9991), pg_proxy)
    s.serve_forever()
