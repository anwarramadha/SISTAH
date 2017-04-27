#!/usr/bin/env python

from threading import Thread
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from aiohttp import ClientSession
from random import randint
from time import sleep
from asyncio import coroutine
import urllib.request
import operator

import sys

PORTS = [13320, 13321, 13322, 13323, 13324]
WORKERS = [13330, 13331, 13332]
vote = 0
timeout = randint(3, 5)
receive_heartbeat = 'N'
cpu_loads = []
role = "FOLLOWER"

class WorkerCpuLoads:
	def __init__(self, worker_port, worker_cpu_load):
		self.port = worker_port
		self.cpu_load = worker_cpu_load

class Server(BaseHTTPRequestHandler):

	def do_GET(self):
		global timeout, cpu_loads, role
		try:
			args = self.path.split('/')
			if len(args) != 2:
				raise Exception()

			req = args[1]
			
			# jika node menerima heartbeat dari leader, yang 
			# menandakan leader belum crash
			if (req == 'heartbeat'):
				if role == 'CANDIDATE':
					role = 'FOLLOWER'
				print("Leader still alive")
				self.response("Y")
				timeout = randint(3, 5)
				receive_heartbeat = 'Y'
			elif req == 'vote':
				self.response("vote")
			else:
				data = int(args[1])

				if role == 'LEADER':
					url = "http://localhost:"+str(cpu_loads[0].port)+"/"+str(data)
					response = urllib.request.urlopen(url).read()

					prime_number = response.decode('utf-8')

					self.response(prime_number)


		except Exception as ex:
			self.send_response(500)
			self.end_headers()
			print(ex)

	def response(self, message):
		global vote
		if message == 'vote':
			try:
				self.send_response(200)
				self.end_headers()
				self.wfile.write('voteyou'.encode('utf-8'))
				vote = 1
			except:
				pass
		else:
			try:
				self.send_response(200)
				self.end_headers()
				self.wfile.write(message.encode('utf-8'))
			except:
				pass


class Client:
	def __init__(self):
		self.count = 0
		self.leader_timeout = randint(1, 2)

	def run(self):
		Thread(target=self.timeout).start()
		Thread(target=self.leader_timeout_counter).start()

	# kalau suatu node menjadi leader, maka setiap periode
	# tertentu, node ini akan mengirimkan hearbeat kesemua
	# node yang menjadi follower, untuk memberitahu bahwa
	# dirinya belum crash
	def broadcast_heartbeat(self):
		print("send heartbeat")
		for port in PORTS:
			if port != PORT:
				url = "http://localhost:"+str(port)+"/heartbeat"
				try:
					urllib.request.urlopen(url).read()
				except:
					pass

		global cpu_loads
		cpu_loads = []
		for port in WORKERS:
			url = "http://localhost:"+str(port)+"/reqcpuloads"
			try:
				response = urllib.request.urlopen(url).read()
				cpu_load = float(response.decode('utf-8'))

				worker = WorkerCpuLoads(port, cpu_load)
				cpu_loads.append(worker)
			except:
				pass

		cpu_loads.sort(key=operator.attrgetter('cpu_load'))

	# apabila pada waktu tertentu leader tidak mengirimkan
	# heartbeat, maka diasumsikan leader telah crash, sehingga
	# harus ada leader baru. Untuk itu node sebagai follower
	# dapat mencalonkan diri sebagai leader yang baru dan
	# mengirimkan pesan vote. Setelah node menerima 1/2n + 1
	# suara dari jumlah server yang tersedia, maka node dapat
	# menjadi leader.
	def do_Campaign(self):
		# print("do campaign")
		global role
		role = "CANDIDATE"
		for port in PORTS:
			if port != PORT:
				url = "http://localhost:"+str(port)+"/vote"
				try:
					response = urllib.request.urlopen(url).read()
					data = response.decode('utf-8')
					# print(self.count)
					if data == 'voteyou':
						self.count += 1
				except Exception as ex:
					# print(ex)
					pass

		if self.count > 1/2 * len(PORTS):
			self.become_leader()


	def become_leader(self):
		print("become leader")
		global role
		role = "LEADER"
		self.broadcast_heartbeat()

	def timeout(self):
		global timeout, receive_heartbeat, vote, role
		while True:
			timeout -= 1
			if timeout == 0:
				print(role)

				if receive_heartbeat == 'N':
					self.do_Campaign()
					timeout = randint(3, 5)
				else:
					timeout = randint(3, 5)

				vote = 0
				self.count = 0
			sleep(1)

	def leader_timeout_counter(self):
		global role
		while True:
			if role == 'LEADER':
				self.leader_timeout -= 1
				if self.leader_timeout == 0:
					self.broadcast_heartbeat()
					self.leader_timeout = randint(1, 2)

			sleep(1)

PORT = int(sys.argv[1])

if __name__ == '__main__':
	thread = Client()
	thread.run()

	server = HTTPServer(("", PORT), Server)
	server.serve_forever()
