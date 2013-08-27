from twisted.internet import reactor,defer,task
from txredis.client import RedisClient
from twisted.internet.error import ConnectionLost,ConnectionDone,ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
HOST = '127.0.0.1'
PORT = 6379



class clientRedis:

	def __init__(self):
		self._deferlist = []
		self.redis = None
		self.stopTrying = False
		self.ready = False
		self.__connect()
		
	@defer.inlineCallbacks
	def __connect(self):
		print "lancement de la connection a redis"
		retry = 10
		while not self.stopTrying:
			try: 
				self.redis = yield ClientCreator(reactor, RedisClient).connectTCP(HOST, PORT)
				self.ready = True
			except ConnectionRefusedError:
				print "connection impossible"
				if not retry:
					print "nombre d'essai fini on reessais dans 1 heure"
					retry = 11
					yield task.deferLater(reactor, 3600,lambda: None)
				retry -= 1
				print "on essais dans %i secondes"%(10 - retry)
				yield task.deferLater(reactor, (10 - retry),lambda: None)
			else:
				defer.returnValue(True)
	
	@defer.inlineCallbacks
	def __call__(self,command,*args):
		# quand on est pret
		result = None
		while not self.ready:
			yield task.deferLater(reactor, 1,lambda: None)
		while result == None:
			try:
				result = yield getattr(self.redis, command)(*args)
				#print "le res : ",result
			except (ConnectionLost,ConnectionDone,ConnectionRefusedError,RuntimeError),e:
				print e
				self.ready = False
				yield self.__connect()
			except Exception,e:
				raise
		defer.returnValue(result)

if __name__ == '__main__':
	from time import sleep

	def printit(r):
		print r
	
	def test():
		coop = task.Cooperator()
		from os import getpid
		import psutil
		me = psutil.Process(getpid())
		print "memoire %i"%(me.get_memory_info().rss/1024)
		cr = clientRedis()
		d = cr("ping")
		d.addCallbacks(printit,printit)
		def iterate():
			i= 0 
			d = cr("set","test",0)
			d.addCallbacks(printit,printit)
			while True:
				print "on envoi %i"%i
				d = cr("incr","test")
				d.addCallbacks(printit,printit)
				d = cr("get","test")
				d.addCallbacks(printit,printit)
				i += 1
				print "memoire %i"%(me.get_memory_info().rss/1024)
				sleep(0.1)
				yield
		coop.coiterate(iterate())
		
	print "allez!!!"
	
	
	reactor.callWhenRunning(test)
	reactor.run()