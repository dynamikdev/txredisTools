from txredisTools.cache import CacheSystem
from txredisTools.redisClient import clientRedis
from twisted.trial import unittest
from twisted.internet import defer,reactor,task

class CacheSystemTest(CacheSystem):
    namecache="TEST"


class CacheSystemTestCase(unittest.TestCase):
    
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self,methodName)
    @defer.inlineCallbacks
    def setUp(self):
        self.cache = CacheSystemTest()
        self.dicToTest = {"a":"toto","b":"titi"}
        yield self.cache.initialiseCache({"a":"toto","b":"titi"})
        defer.returnValue(True)
    def tearDown(self):
        pending = reactor.getDelayedCalls()
        active = bool(pending)
        for p in pending:
            print p
            if p.active():
                print "cancel"
                p.cancel()
        print "fini"
    @defer.inlineCallbacks    
    def test_getitem(self):
        self.cache.data = {}
        yield self.cache.getitem("a")
        with self.cache["a"] as result:
            self.assertEqual(result, "toto")
        yield self.cache.getitem("c")
        with self.cache["c"] as result:
            self.assertEqual(result, None)
    @defer.inlineCallbacks
    def test_getAll(self):
        allitems = yield self.cache.getAll()
        for item in ["a","b"]:
            self.assertEqual(self.dicToTest[item], allitems[item])
        self.assertEqual(len(self.dicToTest), len(allitems))
    @defer.inlineCallbacks
    def test_setitem(self):
        yield self.cache.setitem("a","prout")
        with self.cache["a"] as result:
            self.assertEqual(result, "prout")
        
    @defer.inlineCallbacks
    def test_delete(self):
        yield self.cache.delitem("b")
        with self.cache["b"] as result:
            self.assertEqual(result, None)
    @defer.inlineCallbacks
    def test_exists(self):
        result = yield self.cache.has_key("b")
        self.assertEqual(result, True)
        result = yield self.cache.has_key("c")
        self.assertEqual(result, False)
    @defer.inlineCallbacks
    def test_deleteCache(self):
        result = yield self.cache.deleteCache()
        self.assertEqual(result, True)
        result = yield self.cache.has_key("c")
        self.assertEqual(result, False)
    @defer.inlineCallbacks
    def test_usingCache(self):
        with self.cache["a"] as element:
            self.assertEqual(element, "toto")
            element = "prout"
            self.assertNotEqual(element, "toto")
        with self.cache["a"] as element:
            self.assertEqual(element, "prout")
        yield self.cache.getitem("a")
        with self.cache["a"] as result:
            self.assertEqual(result, "prout")
    @defer.inlineCallbacks
    def test_withCamion(self):
        import arpe.camion
        camion = arpe.camion.Camion(codcam="testCam",
                                    codsoc="codsoc",
                                    idembarq="0316548|54",
                                    idcamion=123546,
                                    boitier=None,
                                    immat="0313265ml13")
        yield self.cache.setitem("123546",camion)
        yield self.cache.getitem("123546")
        with self.cache["123546"] as cam:
            self.assertEqual(cam, camion)
            cam.idembarq = "caca"
            
        with self.cache["123546"] as cam:
            self.assertNotEqual(cam, camion)
            print cam
            self.assertNotEqual(cam.idembarq, camion.idembarq)
            print camion
    @defer.inlineCallbacks
    def test_withAllCamionPrimary(self):
        import arpe.camion
        import arpe.registre
        arpe.registre.initialiseOnTwisted()
        import arpe.interfaces
        import arpe.boitier
        from zope.component import queryAdapter,getUtility
        class CacheSystemTestCamion(CacheSystem):
            namecache="CAMION"
            def initialiseCache(self):
                adp = queryAdapter( arpe.camion.ListeCamion(),arpe.interfaces.ISelectableByBoitier)
                def initCache(lc):
                    dicToCache = {}
                    for cam in lc:
                        if not dicToCache.has_key(cam.idembarq):
                            dicToCache[cam.idembarq] = []
                        dicToCache[cam.idembarq].append(cam)
                    return CacheSystem.initialiseCache(self,dicToCache)
                return adp.get(arpe.boitier.Boitier(idembarq="%")).addCallback(initCache)
        cache = CacheSystemTestCamion()
        yield cache.initialiseCache()
        cam = arpe.camion.Camion(codcam="testCam",
                                    codsoc="codsoc",
                                    idembarq="0316548|54",
                                    idcamion=123546,
                                    boitier=None,
                                    immat="0313265ml13")
        with cache["AX00003114"] as camions:
            print camions
            camions.append(cam)
            del(camions[0])
            print camions
        with cache["AX00003114"] as camions:
            print camions
    @defer.inlineCallbacks
    def test_withAllCamionSecondary(self):
        import arpe.camion
        import arpe.registre
        import arpe.interfaces
        import arpe.boitier
        from zope.component import queryAdapter,getUtility
        class CacheSystemTestCamionSec(CacheSystem):
            namecache="CAMION"
            def initialiseCache(self):
                d = CacheSystem.initialiseCache(self)
                return d.addCallback(lambda r:self.getAll())
        cache = CacheSystemTestCamionSec()
        yield cache.initialiseCache()
        cam = arpe.camion.Camion(codcam="testCam",
                                    codsoc="codsoc",
                                    idembarq="0316548|54",
                                    idcamion=123546,
                                    boitier=None,
                                    immat="0313265ml13")
        with cache["AX00003114"] as camions:
            print camions
            camions.append(cam)
            del(camions[0])
            print camions
        with cache["AX00003114"] as camions:
            print camions
    @defer.inlineCallbacks
    def test_updatePropagation(self):
        yield self.cache.setitem("toto","testouille")
        yield task.deferLater(reactor,1,lambda: None)
        with self.cache["toto"] as elem:
            self.assertEqual(elem,"testouille")
        
        
        