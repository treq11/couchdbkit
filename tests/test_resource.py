# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.
#
__author__ = 'benoitc@e-engura.com (Benoît Chesneau)'

import unittest

from couchdbkit.errors import RequestFailed
from couchdbkit.resource import CouchdbResource


class ServerTestCase(unittest.TestCase):
    def setUp(self):
        self.couchdb = CouchdbResource()
        try:
            self.couchdb.delete('/couchdkbit_test')
        except:
            pass
        
    def tearDown(self):
        self.couchdb = None
        try:
            self.couchdb.delete('/couchdkbit_test')
        except:
            pass

    def testGetInfo(self):
        info = self.couchdb.get().json_body
        self.assert_(info.has_key('version'))
        
    def testCreateDb(self):
        res = self.couchdb.put('/couchdkbit_test').json_body
        self.assert_(res['ok'] == True)
        all_dbs = self.couchdb.get('/_all_dbs').json_body
        self.assert_('couchdkbit_test' in all_dbs)
        self.couchdb.delete('/couchdkbit_test')

    def testRequestFailed(self):
        bad = CouchdbResource('http://localhost:10000')
        self.assertRaises(RequestFailed, bad.get)
        
if __name__ == '__main__':
    unittest.main()


