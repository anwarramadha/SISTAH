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

# bagian ini yang harus disimpan adalah IP address karena yang 
# digunakan beda komputer
WORKERS = ['http://192.168.43.142', 'http://localhost']
DAEMONS = ['http://192.168.43.142', 'http://localhost']

timeout = randint(3, 5)
receive_heartbeat = 'N'
cpu_loads = []
role = "FOLLOWER"
forward = 0
leader_port = None
leader_before = 0
count_leader = 0
term = 0

append_entries = []

class WorkerCpuLoads:
	def __init__(self, ip, worker_cpu_load):
		self.ip = ip
		self.cpu_load = worker_cpu_load

class Server(BaseHTTPRequestHandler):

	def do_GET(self):
		global timeout, cpu_loads, role, leader_port, term, receive_heartbeat, count_leader, leader_before
		try:
			args = self.path.split('/')
			if len(args) < 2:
				raise Exception()

			req = args[1]
			if req == 'vote' and len(args) > 2:
				new_term = int(args[2])
				if role == 'LEADER':
					role = 'FOLLOWER'

			# jika node menerima heartbeat dari leader, yang 
			# menandakan leader belum crash
			if (req == 'heartbeat'):
				if role == 'CANDIDATE':
					role = 'FOLLOWER'
				print("Leader still alive")
				self.response("Y")
				timeout = randint(3, 5)
				receive_heartbeat = 'Y'
				if count_leader==0:
					fileHandle = open (str(PORT)+".txt","r" )
					lineList = fileHandle.readlines()
					fileHandle.close()
					if len(lineList)!=0:
						count_leader = int(lineList[-1])
						leader_before = int(lineList[len(lineList)-2])

				if leader_before != leader_port and leader_port!=None:
					count_leader=int(args[3])
					leader_before=leader_port
					file = open(str(PORT)+".txt","a") 
					file.write("%s\n" % str(leader_port)) 
					file.write("%s\n" % str(count_leader))
					file.close() 
				leader_port = int(args[2])
				
				

			# Jika leader crash, maka setiap node yang telah
			# habis waktu timeoutnya, otomatis akan menjadi 
			# candidate. Setiap candidate, harus mengirim 
			# request vote kesemua node agar mendapat mayoritas
			# suara. Suara yang didapat akan digunakan sebagai
			# perhitungan agar menjadi leader.
			elif req == 'vote' and new_term != term and new_term > term:
				# if role == 'FOLLOWER':
				if count_leader <= int(args[3]):
					self.response("voteyou")
					term = new_term
				else:
					self.response("notvote")

			elif req == 'appendentries':
				file = open(str(PORT)+"-cpuloads.txt","a") 
				file.write(args[4]+"\n")
				file.write(args[3]+"\n")
				file.close() 

			# Bagian ini adalah request dari client yang berisi
			# angka yang ingin dicari angka primanya
			# Jika yang mendapat request adalah leader, maka
			# load balancer akan langsung mengirim data ke worker,
			# tetapi jika yang menerima adalah follower, maka
			# request akan di forward ke leader.
			else:
				data = args[1]

				if role == 'LEADER':
					url = "http://localhost:"+str(cpu_loads[0].port)+"/"+data
					#url = "http://localhost:"+"13337"+"/"+data
					response = urllib.request.urlopen(url).read()

					prime_number = response.decode('utf-8')

					self.response(prime_number)
				elif role == 'FOLLOWER':
					url = "http://localhost:"+str(leader_port)+"/"+data
					response = urllib.request.urlopen(url).read()

					prime_number = response.decode('utf-8')

					self.response(prime_number)

		except Exception as ex:
			self.send_response(500)
			self.end_headers()
			print(ex)

	def response(self, message):
		if message == 'vote':
			try:
				self.send_response(200)
				self.end_headers()
				self.wfile.write('voteyou'.encode('utf-8'))
			except:
				pass
		elif message == 'notvote':
			try:
				self.send_response(200)
				self.end_headers()
				# self.wfile.write(message.encode('utf-8'))
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
		self.daemon_count = len(DAEMONS)
		self.leader_timeout = 1

	# thread 
	def run(self):
		Thread(target=self.timeout).start()
		Thread(target=self.leader_timeout_counter).start()
		Thread(target=self.request_cpu_loads).start()

	# kalau suatu node menjadi leader, maka setiap periode
	# tertentu, node ini akan mengirimkan hearbeat kesemua
	# node yang menjadi follower, untuk memberitahu bahwa
	# dirinya belum crash
	def broadcast_heartbeat(self):
		print("send heartbeat")
		global cpu_loads, term, count_leader 

		for port in PORTS:
			if port != PORT:
				url = "http://localhost:"+str(port)+"/heartbeat/"+str(PORT)+"/"+str(count_leader)
				try:
					urllib.request.urlopen(url).read()
				except:
					pass

	# apabila pada waktu tertentu leader tidak mengirimkan
	# heartbeat, maka diasumsikan leader telah crash, sehingga
	# harus ada leader baru. Untuk itu node sebagai follower
	# dapat mencalonkan diri sebagai leader yang baru dan
	# mengirimkan pesan vote. Setelah node menerima 1/2n + 1
	# suara dari jumlah server yang tersedia, maka node dapat
	# menjadi leader.
	def do_Campaign(self):
		print("do campaign")
		global role, term, count_leader
		term += 1
		self.count = 1
		role = "CANDIDATE"
		print(count_leader)
		for port in PORTS:
			if port != PORT:
				url = "http://localhost:"+str(port)+"/vote/"+str(term)+"/"+str(count_leader)
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
		else:
			self.count = 0
	# Jika candidate berhasil mendapat vote mayoritas
	# maka node yang berstatus candidate akan menjadi 
	# leader
	def become_leader(self):
		print("become leader")
		global role, count_leader
		count_leader += 1
		role = "LEADER"
		self.broadcast_heartbeat()

	# timeout node, tapi bukan milik leader
	def timeout(self):
		global timeout, receive_heartbeat, role
		while True:
			timeout -= 1
			# print("gg")
			if timeout == 0 :
				if role == 'FOLLOWER':
					print(role)

					if receive_heartbeat == 'N':
						# print("ff")
						self.do_Campaign()
						timeout = randint(3, 5)

					self.count = 0
				elif role == 'CANDIDATE':
					role = 'FOLLOWER'
					timeout = randint(3, 5)
				# if role == 'LEADER':
				# 	role = 'FOLLOWER'
			if receive_heartbeat == 'Y':
				receive_heartbeat = 'N'
			sleep(1)

	# timeout counter yang digunakan sebagai penghitung mundur
	# untuk mengirim heartbeat ke follower
	def leader_timeout_counter(self):
		global role
		while True:
			if role == 'LEADER':
				print("hhh")
				self.leader_timeout -= 1
				if self.leader_timeout == 0:
					self.broadcast_heartbeat()
					self.leader_timeout = 1

			sleep(1)

	def request_cpu_loads(self):
		global append_entries, cpu_loads, role, count_leader, leader_before
		self.timeout = 5
		while True:
			self.timeout -= 1
			self.majority_consistent = 0
			commited = False
			if self.timeout == 0:
				if role == 'LEADER':
					print("ggg")
					if count_leader==0:
						fileHandle = open (str(PORT)+".txt","r" )
						lineList = fileHandle.readlines()
						fileHandle.close()
						if len(lineList)!=0:
							count_leader = int(lineList[-1])
							leader_before = int(lineList[len(lineList)-2])

					if leader_before!=PORT and PORT!=None:
						leader_before=PORT
						file = open(str(PORT)+".txt","a") 
						file.write("%s\n" % str(PORT)) 
						file.write("%s\n" % str(count_leader))
						file.close() 
					
					cpu_loads = []
					daemon_count = 0
					for ip in DAEMONS:
						url = ip+":"+str(DAEMONS)+"/reqcpuloads"
						try:
							response = urllib.request.urlopen(url).read()
							cpu_load = float(response.decode('utf-8'))

							worker = WorkerCpuLoads(ip, cpu_load)
							# worker = WorkerCpuLoads(WORKERS[DAEMONS.index(port)], cpu_load)
							cpu_loads.append(worker)
						except:
							daemon_count += 1

					if daemon_count == self.daemon_count:
						self.get_cpu_loads_from_file()

					cpu_loads.sort(key=operator.attrgetter('cpu_load'), reverse=True)

					for port in PORTS:
						for item in cpu_loads:
							url = "http://localhost:"+str(port)+"/appendentries/"+str(port)+"/"+str(item.cpu_load)+"/"+item.ip

							try:
								response = urllib.request.urlopen(url).read()
								print("append")
								
							except:
								pass
					# ketika leader menerima cpu loads dari daemon, maka entries akan
					# ditambahkan kedalam log leader, namun belum di commit, yang artinya
					# entries belum di tulis kedalam file.
					file = open(str(PORT)+"-cpuloads.txt","w") 
					for item in cpu_loads:
						file.write("%s\n" % str(item.ip))
						file.write("%s\n" % str(item.cpu_load))
					
					file.close()
					# Simpan siapa ketua dan log cpu load
				self.timeout = 5

				self.cpu_loads_received = 'Y'
			sleep(1)

	def get_cpu_loads_from_file():
		global cpu_loads
		fileHandle = open (str(PORT)+"-cpuloads.txt","r" )
		lineList = fileHandle.readlines()
		fileHandle.close()
		return lineList


def readFile():
	global count_leader
	fileHandle = open (str(PORT)+".txt","r" )
	lineList = fileHandle.readlines()
	fileHandle.close()
	if len(lineList)!=0:
		count_leader = int(lineList[-1])
		leader_before = int(lineList[len(lineList)-2])

PORT = int(sys.argv[1])

if __name__ == '__main__':
	readFile()
	thread = Client()
	thread.run()

	server = HTTPServer(("", PORT), Server)
	server.serve_forever()
