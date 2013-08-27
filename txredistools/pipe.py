
from datetime import datetime
class TubeRedis(object):
    def __init__(self,name):
	self.name = name
	self.nbmessages=0
	self.datelastmessage = None
    def _messageReceived(self,message):
	self.nbmessage +=1
	self.datelastmessage=datetime.now()
	self.processMessage(message)
    def processMessage(self,message):
	pass
    
class PipeRedisWorker(object):
    def __init__(self):
	pass
    
	
    def _getCommand(self):
	pass

class PipeRedisCommander(object):
    def __init__(self,prefix=""):
	self.prefix=prefix
	self.tubes = {}
	self.commandes = defer.DeferredQueue()
