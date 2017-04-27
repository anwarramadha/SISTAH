from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import psutil
import os
import sys

PORT = 13340

class WorkerHandler(BaseHTTPRequestHandler):  
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
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

    def cpu_usage(self):
    	proc = psutil.Process(os.getpid())
    	return proc.memory_percent()

server = HTTPServer(("", PORT), WorkerHandler)
server.serve_forever()