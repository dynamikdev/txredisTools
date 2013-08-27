from twisted.internet import defer,task,reactor
from txredis.client import Redis,RedisSubscriber,RedisSubscriberFactory
from twisted.internet.error import ConnectionLost,ConnectionDone,ConnectionRefusedError
from twisted.internet.protocol import ClientCreator
__HOST__ = '127.0.0.1'
__PORT__ = 6379
import sys
class clientRedis(object):

    def __init__(self,db=0,port = __PORT__,unix = None):
        self.db = db
        self.port = port
        self.unix = unix
        self._deferlist = []
        self.redis = None
        self.stopTrying = False
        self.ready = defer.Deferred()
        self.__connect()
        reactor.addSystemEventTrigger('before', 'shutdown',self.shutdownme)
    
    @defer.inlineCallbacks
    def __connect(self):
        print "lancement de la connection a redis",self.port," - ",self.unix
        retry = 10
        while not self.stopTrying:
            try: 
                if self.port:
                    self.redis = yield ClientCreator(reactor, Redis).connectTCP(__HOST__, self.port)
                elif self.unix:
                    self.redis = yield ClientCreator(reactor, Redis).connectUNIX(self.unix)
                else:
                    raise NotImplemented("PAS de port ou de socket fournit au client")
                r = yield self.redis.select(self.db)
                print r , self,self.port,self.unix
                self.ready.callback(True)
                
            except ConnectionRefusedError,e:
                print >>sys.stderr,"connection impossible",str(e)
                if not retry:
                    print "nombre d'essai fini on reessais dans 1 heure"
                    retry = 11
                    yield task.deferLater(reactor, 3600,lambda: None)
                retry -= 1
                print "on essais dans %i secondes"%(10 - retry)
                yield task.deferLater(reactor, (10 - retry),lambda: None)
            except Exception,e:
                print >>sys.stderr,"connection impossible sur ",self.port," ", self.unix," ",str(e)
                print "connection impossible sur ",self.port," ", self.unix," ",str(e)
                raise e
            else:
                
                defer.returnValue(True)


    def hardUnplug(self):
        if self.redis:
            return self.redis.transport.loseConnection()
    def shutdownme(self):
        self.stopTrying = True
        if self.redis:
            return self.redis.quit()
    @defer.inlineCallbacks
    def __call__(self,command,*args,**kwargs):
        # quand on est pret
        # print "on appelle ",command,args
        # print "sur",self
        result = None
        # print self.ready
        try:
            r = yield self.ready
            # print "------>",r
        except Exception,e:
            print e
        # print "comme on est pres...."
        while not self.stopTrying:
            try:
                result = yield getattr(self.redis, command)(*args,**kwargs)
                # print "le res : ",result
                break
            except (ConnectionLost,ConnectionRefusedError,RuntimeError),e:
                print "erreur de connection dans le call",e
                self.ready = defer.Deferred()
                try:
                    yield self.__connect()
                except Exception,e :
                    print "erreur au connect",e
            except ConnectionDone:
                break
            except Exception,e:
                print >>sys.stderr,"erreur dans le call Redis", e,command,args,kwargs
                defer.returnValue(False)
        defer.returnValue(result)


    def __getattr__(self,name):
        if hasattr(self.redis,name):
            def wrapped(*args,**kwargs):
                return self(name,*args,**kwargs)
            return wrapped
        else:
            raise AttributeError(name+" not exist un redisclient",self.redis)


class BaseSingletonRedis(object):
    __instances = {}
    @staticmethod
    @defer.inlineCallbacks
    def getInstance(name,configclient = {"db":0}):
        try:
            if name not in BaseSingletonRedis.__instances:
                BaseSingletonRedis.__instances[name] = clientRedis(**configclient)
            yield BaseSingletonRedis.__instances[name].ready
            defer.returnValue(BaseSingletonRedis.__instances[name])
        except Exception,e:
            print e
            raise e
        
        
@defer.inlineCallbacks
def getReadyClient(options= {"db":0}):
    redis = clientRedis(**options)
    yield redis.ready
    defer.returnValue(redis)

        
    
class MyRedisSubscriber(RedisSubscriber):
    def __init__(self, *args,**kwargs):
        dictOfcallback = kwargs.pop("dictOfcallback",{})
        self.dictOfcallback = dictOfcallback
        RedisSubscriber.__init__(self,*args,**kwargs)
        
    
    def messageReceived(self, channel, message):
        self.dictOfcallback.get(channel)(channel,message)
        pass
    #print channel,message
    def connectionMade(self):
        d = RedisSubscriber.connectionMade(self)
        d.addCallback(lambda r:self.subscribeChannels())
    def channelSubscribed(self, channel, numSubscriptions):
        #print "souscrit a ",channel,numSubscriptions
        pass
    

    def subscribeChannels(self):
        self.subscribe(*self.dictOfcallback.keys())
        
class MyClientRedisSubscriber(RedisSubscriberFactory):
    protocol=MyRedisSubscriber
    def __init__(self,*args, **kwargs):
        RedisSubscriberFactory.__init__(self,*args,**kwargs)
        
#reactor.connectTCP("127.0.0.1", 6379, Mycl())
#reactor.run()  
def subscribeToRedis(reactor,dictOfcallback):
    return reactor.connectTCP(__HOST__, __PORT__, MyClientRedisSubscriber(dictOfcallback=dictOfcallback))       



        
class SingletonRedis(BaseSingletonRedis):
    @staticmethod
    def getForThisConfig(nom,config):
        return BaseSingletonRedis.getInstance(nom,config)


    
if __name__ == '__main__':
    pass