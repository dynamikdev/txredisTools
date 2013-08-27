# import os, signal
import psutil
from twisted.internet import protocol, defer, reactor, process
from txredisTools.redisClient import SingletonRedis  
import pprint 

ARGCLIENTREDISCONFIG = {"db":0,"port":False,"unix":"/tmp/redisEmbarqManagerServeur.sock"}

# def shutdown():
#     print "shutting down in 3 seconds"
#     d = defer.Deferred()
#     reactor.callLater(3, d.callback, 1)
#     return d

# # reactor.addSystemEventTrigger("before", "shutdown", shutdown)
# def changeINT2TERM(num,frame):
#     print "SIGNAL RECU!!!!!!",num,frame
#     pprint.pprint(frame)
#     signal.signal(signal.SIGINT, signal.SIG_IGN)
#     os.kill(os.getpgid(os.getpid()), signal.SIGTERM)

# signal.signal(signal.SIGINT, changeINT2TERM)

from twisted.internet.protocol import ProcessProtocol


class RedisLauncher(process.Process):
    def __init__(self,args,proto):
        process.Process.__init__(self,reactor, "redis-server",["redis-server"]+args,{},None,proto)

    # def signalProcess(self, signalID):
    #     """
    #     override pour eviter que le ctrl +c empeche la fermeture correcte
    #     """
    #     print "On a recu le signal a transmetre c ",signalID
    #     if signalID == 'INT' or signalID == 2:
    #         process.Process.signalProcess(self, "TERM")
    #     else:
    #         process.Process.signalProcess(self, signalID)

class Server(protocol.ProcessProtocol):
    def __init__(self,name,conf = {}):
        self.name = name
        self.started = defer.Deferred()
        self.stopped = defer.Deferred()
        self.verbose = conf.get("verbose",True)
        self.dir = conf.get("dir")
        self.dbfilename = conf.get("dbfilename")
        self.databases = conf.get("databases","1")
        self.unixsocket = conf.get("unixsocket")
        self.closing = False
        self.clientConf = {"db":0,"port":False,"unix":self.unixsocket}
        self.process = RedisLauncher(map(str,[
                             "--dir", self.dir,
                            "--dbfilename", self.dbfilename,
                            "--databases" ,self.databases,
                            "--port","0",
                            "--unixsocket",self.unixsocket
                             ]),self)
    def shutdownMe(self):
        self.closing = True
        def printitEnd(arg):
            self.process.signalProcess("TERM")
        return self.getRedisClient().addCallback(lambda r: r.shutdown()).addBoth(printitEnd)

    def getRedisClient(self):
        return  SingletonRedis.getForThisConfig(self.name,self.clientConf)

    def outReceived(self, data):
        if self.verbose:
            print "outReceived! with %d bytes!" % len(data)
            print data
        if "ready" in data:
            if not self.closing:
                if self.verbose:
                    print "LANCE CALLBACK START"
                self.started.callback(True)
            else:
                self.stopped.callback(True)

    def errReceived(self, data):
        print "errReceived! with %d bytes!" % len(data)
        print data 

    def processExited(self, reason):
        if self.verbose:
            print "processExited, status ", reason
        self.stopped.callback(reason)

    
    def makeConnection(self,p):
        print "process ",p," de ",self.name," ok"
        self.process_test = p
        self.processinfo = psutil.Process(p.pid)
        ProcessProtocol.makeConnection(self,p)
    def connectionMade(self):
        print "connection o process"
        # self.onStart.callback(True)