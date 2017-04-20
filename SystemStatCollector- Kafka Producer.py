from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import psutil
import socket
import time
import json
import httplib
import urllib
# importing the requests library
import requests
import json
import fcntl
import struct

def get_ip_address(ifname):
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	return socket.inet_ntoa(fcntl.ioctl(
		s.fileno(),
		0x8915,  # SIOCGIFADDR
		struct.pack('256s', ifname[:15])
	)[20:24])

producer = KafkaProducer(bootstrap_servers = ['192.168.12.57:9092']) # configure kafka IP Address
def processData():
	#Getting cpu percent
	for x in range(1):
		cpupercent = psutil.cpu_percent(interval=1)


	#getting virtual memory
	totalvm = psutil.virtual_memory().total
	availablevm = psutil.virtual_memory().available
	usedvm = psutil.virtual_memory().used
	freevm = psutil.virtual_memory().free

	#getting network details
	tt = psutil.net_io_counters()
	bytessent = psutil.net_io_counters().bytes_sent
	bytesrec = psutil.net_io_counters().bytes_recv
	packetssent = psutil.net_io_counters().packets_sent
	packetsrecv = psutil.net_io_counters().packets_recv	

	#get IP Address of the machine
	ipaddress = get_ip_address('eth0')
	



    # get current time
	time = str(datetime.now())

	data = {

		"ip": ipaddress,
		"cpupercent": cpupercent,
		"totalvm": totalvm,
		"availablevm": availablevm,
		"usedvm": usedvm,
		"freevm": freevm,
		"bytessent": bytessent,
		"bytesrec": bytesrec,
		"packetssent": packetssent,
		"packetsrecv": packetsrecv,
		"time" : time
	}
	json_str = json.dumps(data)

	producer.send('systemstat', json_str)
	print("Value sent to kafka")
		# block until all async messages are sent
	producer.flush()


while True:
	time.sleep(2)
	processData()
