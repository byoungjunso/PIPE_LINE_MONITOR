#!/usr/bin/env python
# coding: utf-8


import socket 
import struct 
import sys 
import os 
import signal 

SHUTDOWN = False
def shutdown(sigNum, frame) :
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write('Catch Signal : %s' % sigNum)
	sys.stderr.flush()

# signal 
signal.signal(signal.SIGTERM, shutdown)		 # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)		  # sigNum  2 : Interrupt
try : signal.signal(signal.SIGHUP, shutdown)	# sigNum  1 : HangUp
except : pass
try : signal.signal(signal.SIGPIPE, shutdown)   # sigNum 13 : Broken Pipe
except : pass

class SmsClient :

	def __init__( self ) :

		self.sms_host = #SMS_SERVER_IP
		self.sms_port = #SMS_SERVER_PORT

		self.system_phone	= #송신자 전화번호
		self.system_name	= #송신자 이름

	def send( self, phone_number, send_msg ) : 

		s = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )

		# korean encoding
		packet = struct.pack( '12s12s100s40s40s200s', ( phone_number ).encode(), self.system_phone.encode(), self.system_name.encode(), 'ETC'.encode(), 'ETC'.encode(), send_msg.encode('euckr') )

		s.sendto( packet, ( self.sms_host, self.sms_port ) ) 

		print('Send SMS %s Done - %s' % ( phone_number, send_msg ))

		s.close()

 
def usage() :
	print('Usage) python %s {phone number} {send message}' % sys.argv[0])
	sys.exit()
 
def main() : 
	
	if len( sys.argv ) < 3: 
		usage()

	phone_number	= sys.argv[1]
	send_message	= sys.argv[2]

	sms = SmsClient()
	sms.send( phone_number, send_message )
 
if __name__ == '__main__' : 
	main() 
