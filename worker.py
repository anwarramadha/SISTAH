#!/usr/bin/env python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import psutil
import os
import sys

class WorkerHandler(BaseHTTPRequestHandler):
    def prime(self, n):
        i = 2
        while i * i <= n:
            if n % i == 0:
                return False
            i += 1
        return True

    def calc(self, n):
        p = 1
        while n > 0:
            p += 1
            if self.prime(p):
                n -= 1
        return p

    def do_GET(self):
        try:
            args = self.path.split('/')
            if len(args) != 2:
                raise Exception()

            command = args[1]
            if command == 'reqcpuloads':
            	self.send_response(200)
            	self.end_headers()
            	self.wfile.write(str(self.cpu_usage()).encode('utf-8'))
            else:
            	n = int(args[1])
            	self.send_response(200)
            	self.end_headers()
            	self.wfile.write(str(self.calc(n)).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

    def cpu_usage(self):
    	proc = psutil.Process(os.getpid())
    	return proc.memory_percent()

PORT = int(sys.argv[1])

server = HTTPServer(("", PORT), WorkerHandler)
server.serve_forever()
