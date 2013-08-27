from twisted.internet import defer,task,reactor, protocol
#from txredis.protocol import Redis
from itertools import dropwhile
from txredisTools.redisClient import clientRedis,subscribeToRedis
#try:
    #import ujson as json
#except:
    #import json
#else:
    #import simplejson as json
try:
    import cPickle as pickle
except ImportError:
    import pickle   
from copy import copy
#class MetaRedisCache(type):
    #def __new__(cls, name, bases, attrs):
	#assert attrs["_prefix"]!="NOTGIVED::" or name != "RedisCache", "Vous devez specifier un prefix pour votre system de cache"
	#return type.__new__(cls, name, bases, attrs) 
class CacheElement(object):
    def __init__(self,obj,cachesys,key):
	self.obj = obj
	assert isinstance(cachesys,CacheSystem),"Ce n'est pas un system de cache"
	self.cachesys = cachesys
	self.key = key
    def __eq__(self,other):
	if isinstance(other,CacheElement):
	    return self.obj == other.obj
	else:
	    return self.obj == other
	
    def __enter__(self):
	self.saveObject = pickle.dumps(self.obj)
	return self.obj
    def __exit__(self, type, value, traceback):
	if self.saveObject != pickle.dumps(self.obj):
	    self.cachesys.setitem(self.key,self.obj)
	return self.obj
    
class CacheElementNotPresentException(Exception):
    pass
class CacheElementNotPresent(object):
    def __enter__(self):
	return None
    def __exit__(self, type, value, traceback):
	return True
    

class CacheSystem(object):
    _prefix = "TXREDISTOOL_CACHESYSTEM::"
    namecache="GLOBAL"
    redisclient = None
    data = {}
    def __init__(self,dbRedis = 0):
	self.redisclient = clientRedis(dbRedis)
        subscribeToRedis(reactor,{self._prefix+self.namecache+"::__RELOAD__":self.reloadItem})
	subscribeToRedis(reactor,{self._prefix+self.namecache+"::__DELETE__":self.haveToDeleteItem})
    def haveToDeleteItem(self,channel,message):
	print "!!!!!! On supprime l'item",message
	del(self[message])
    def reloadItem(self,channel,message):
	print "!!!!On recharge l'Item",message
	return self.getitem(message).addCallback(self.reloadedItem)
    def reloadedItem(self,item):
	with item as el:
	    print "item recharge:",el
    @defer.inlineCallbacks
    def initialiseCache(self, dicttocache={}):
	yield self.redisclient.ready
	if len(dicttocache):
	    self.data = dict(map(lambda k : (k,CacheElement(dicttocache[k],self,k)),dicttocache))
	    for i in dropwhile( lambda k:isinstance(k, basestring),dicttocache):
		raise RuntimeError("The key Must Be a string")
	    result = yield self.storeData()
	else:
	    result = None
	defer.returnValue(result)
    
    @defer.inlineCallbacks
    def checkModifCache(self,dicttocache={}):
	for k in dicttocache:
	    if self.data.has_key(k):
		if self.data[k] == dicttocache[k]:
		    continue
	    yield self.setitem(k,dicttocache[k])
	defer.returnValue(True)	
	    
    #def setredisclient(self,redisclient):
        #self.redisclient = redisclient	
	
    #def getredisclient(self):
        #return self.redisclient 
    def getROElement(self,*args):
	d = {}
	for el in args:
	    e = self[el].__enter__()
	    if e:
		d[el] = e
	return d
    
    def getitem(self,key):
	assert isinstance(key, basestring),"The key Must Be a string"
        def setElemAndReturn(r):
	    if r != None:
		self[key] = CacheElement(pickle.loads(r[key]),self,key) 
	    return self[key]
        return self.redisclient.hget(self._prefix+self.namecache,key).addCallback(setElemAndReturn)
    
    def setitem(self,key,value):
	print "c'est la key",key
	assert isinstance(key, basestring),"The key Must Be a string"
        return self.redisclient.hset(self._prefix+self.namecache,key,pickle.dumps(value)).addCallback(lambda r:self.redisclient.publish(self._prefix+self.namecache+"::__RELOAD__",key))
    
    def delitem(self, key):
        return self.redisclient.hdel(self._prefix+self.namecache,key).addCallback(lambda r:self.redisclient.publish(self._prefix+self.namecache+"::__DELETE__",key))
    def storeData(self):
	print "on y est on store"
	return self.deleteCache().addCallback(lambda r:self.redisclient.hmset(self._prefix+self.namecache,
	                              dict(map(lambda k : (k,pickle.dumps(self.data[k].obj)),
	                                       self.data)
	                                   )
	                              ))
    def getAll(self):
	def setDataAndReturn(result):
	    self.data=dict(map(lambda k : (k,CacheElement(pickle.loads(result[k]),self,k)),result))
	    return dict(map(lambda k : (k,pickle.loads(result[k])),result))
        return self.redisclient.hgetall(self._prefix+self.namecache).addCallback(setDataAndReturn)
    
    def has_key(self,key):
	assert isinstance(key, basestring),"The key Must Be a string"
        return self.redisclient.hexists(self._prefix+self.namecache,key)
    
    def deleteCache(self):
        return self.redisclient.delete(self._prefix+self.namecache)
    def __getitem__(self, key):
	assert isinstance(key, basestring),"The key Must Be a string"
        if self.data.has_key(key):
	    return self.data[key]
	else:
	    return CacheElementNotPresent()
	
    def __setitem__(self, key,value):
	assert isinstance(key, basestring),"The key Must Be a string"
	self.data[key] = value
	
    def __delitem__(self, key):
	assert isinstance(key, basestring),"The key Must Be a string"
	if self.data.has_key(key):
	    del(self.data[key])
    