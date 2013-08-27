from zope.interface import Interface,implements
from zope.component import adapts
from txredisTools.redisClient import SingletonRedis,getForPipe
from outils import Coroutine2,CoroutineRegister
from twisted.internet import defer,reactor,task
from twisted.application.service import Service
from datetime import datetime
from time import mktime,time
import random
import string
try:
    from cPickle import dumps,loads
except ImportError:
    from pickle import dumps,loads

class IProducer(Interface):
    def produce(self,val):
        """ methode de production"""
        pass
class Producer():
    implements(IProducer)
    def __init__(self,namedata,producer=None):
        self._namedata = namedata
        self._lock = defer.DeferredLock()
        self._setOfSubscriber = namedata+".subscribers"
        self._setOfSubscribed = namedata+".subscribed"
        self._dredis = getForPipe().addCallback(self._storeRedisClient)
        if producer != None:
            self.producer = producer
    def _storeRedisClient(self,r):
        self._redis = r
        return r
    def producer(self):
        for i in range(10):
            self.produce(i)
    @defer.inlineCallbacks
    def _manageAskingForSubscribe(self):
        yield self._dredis
        member = yield self._redis("spop",self._setOfSubscriber)
        while member != None:
            yield self._redis("sadd",self._setOfSubscribed,member)
            # on supprime les donnee de plus d un jour
            yield self._redis("zremrangebyscore", self._namedata+".store", "-inf", time()-(60*60*24))
            elemsToSend = yield self._redis("zrange",self._namedata+".store",0,-1)
            #print elemsToSend
            for elem in elemsToSend:
                yield self._redis("rpush",self._namedata+"."+member,elem)
            member = yield self._redis("spop",self._setOfSubscriber)
        defer.returnValue(True)
    @defer.inlineCallbacks    
    def _sendToSubscribers(self,value,date):
        yield self._dredis
        # on sauve plus c'est trop cher
        #yield self._redis("zadd",self._namedata+".store",dumps(value),mktime(date.timetuple()))
        yield self._manageAskingForSubscribe()
        membersSubscribed = yield self._redis("smembers",self._setOfSubscribed)
        for member in membersSubscribed:
            yield self._redis("rpush",self._namedata+"."+member,dumps(value))
        defer.returnValue(True)
        
    @defer.inlineCallbacks    
    def produce(self,val,date=None):
        if date == None:
            date = datetime.now()
        #print "produce ",val
        yield self._lock.run(self._sendToSubscribers,val,date)
        defer.returnValue(True)

class ProducerService(Service,Producer):
    def __init__(self,namedata,producer=None):
        self._namedata = namedata
        self._setOfSubscriber = namedata+".subscribers"
        self._setOfSubscribed = namedata+".subscribed"      
        if producer != None:
            self.producer = producer
    def startService(self):
        self._dredis = getForPipe().addCallback(self._storeRedisClient)
    def stopService(self):
        self._redis("quit")
    
    
class ISubscriber(Interface):
    def suscribe(self):
        pass
    def processing(self,val):
        pass
    def unsubscribe(self):
        pass
    
class Subscriber():
    implements(ISubscriber)
    def __init__(self,namedata,key=None,OnCloseUnSetMe=True):
        self._namedata = namedata
        self._setOfSubscriber = namedata+".subscribers"
        self._setOfSubscribed = namedata+".subscribed"
        self._dredis = getForPipe().addCallback(self._storeRedisClient)
        reactor.addSystemEventTrigger("before", "shutdown", self.unsubscribe)
        if key == None:
            self._mykey = self.generatekey()
            self._keyForced = False
        else:
            self._mykey = key
            self._keyForced = True
        self._OnCloseUnSetMe = OnCloseUnSetMe
        self._running = False
    def _storeRedisClient(self,r):
        self._redis = r
        return r
    def generatekey(self):
        return ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16))
    @defer.inlineCallbacks  
    def subscribe(self):
        try:
            yield self._dredis
            if self._keyForced:
                members = yield self._redis("smembers",self._setOfSubscribed)
                print members
                if members == None or self._mykey not in members:
                    res = yield self._redis("sadd",self._setOfSubscriber,self._mykey)
            else:
                res = yield self._redis("sadd",self._setOfSubscriber,self._mykey)
                while  not self._keyForced and res == 0:
                    self._mykey = self.generatekey()
                    res = yield self._redis("sadd",self._setOfSubscriber,self._mykey)
            self._mylist = self._namedata+"."+self._mykey
            self._running = True
            self._consume()
            defer.returnValue(True)
        except Exception , e:
            raise e
    
    @defer.inlineCallbacks  
    def unsubscribe(self):
        try:
            print "on unsuscrit"
            yield self._dredis
            if self._OnCloseUnSetMe:
                yield self._redis("srem",self._setOfSubscribed,self._mykey)
            yield self._redis("srem",self._setOfSubscriber,self._mykey)
            self._running = False
            defer.returnValue(True)
        except Exception , e:
            raise e
        
    @defer.inlineCallbacks  
    def _consume(self):
        while self._running:
            try:
                redis = yield self._dredis
                val = yield self._redis("bpop",[self._mylist], timeout=1)
                if val != None:
                    self.processing(loads(val[1]))
            except Exception , e:
                raise e
        defer.returnValue(True)
        
    def processing(self,val):
        print "receive "
 
class SubscriberService(Service,Subscriber):
    def __init__(self,namedata,key=None,OnCloseUnSetMe=True):
        self._namedata = namedata
        self._setOfSubscriber = namedata+".subscribers"
        self._setOfSubscribed = namedata+".subscribed"
        #self._dredis = getForPipe().addCallback(self._storeRedisClient)
        #reactor.addSystemEventTrigger("before", "shutdown", self.unsubscribe)
        if key == None:
            self._mykey = self.generatekey()
            self._keyForced = False
        else:
            self._mykey = key
            self._keyForced = True
        self._OnCloseUnSetMe = OnCloseUnSetMe
        self._running = False
    def startService(self):
        self._dredis = getForPipe().addCallback(self._storeRedisClient)
        self.subscribe()
        
    def stopService(self):
        return self.unsubscribe().addCallback(lambda r:self._redis("quit"))
        
    
if __name__ == '__main__':
    from sys import argv
    if argv[1] == "p":
        p = Producer("test")
        #p.producer()
        @defer.inlineCallbacks
        def loop():
            i=0
            while True:
                yield p.produce(i,datetime.now())
                yield task.deferLater(reactor, 1,lambda:None)
                i += 1
        reactor.callWhenRunning(loop)
            
    else:
        #class Montraitementto(Coroutine,Producer):
            #def __init__(self):
                #Coroutine._init__(self,function = self.sendit)
                #Producer._init__(self,"position2")
            #def sendit(self,position):
                #self.produce(position)
        #sto = Montraitementto()
        
        #class Montraitement2(Coroutine):
            #def __init__(self):
                #Coroutine._init__(self,self.change,to=[sto])
            #def change(self,position):
                #print "position pour",position["idembarq"]
        #s2 = Montraitement2()
        
        
        
        class Montraitement1(Subscriber):
            def __init__(self,key):
                Subscriber._init__(self,"test",key,False)
                self.subscribe()
            def processing(self,position):
                print "j'ai",position
        s1 = Montraitement1(argv[1])
        
        
    #@defer.inlineCallbacks 
    #def launch():
        #yield s0.subscribe()
        #yield s1.subscribe()
        #yield s2.subscribe()
        #yield p.produce()
        #yield s0.unsubscribe()
        #yield s1.unsubscribe()
        #yield s2.unsubscribe()
        
    #launch()
    reactor.run()