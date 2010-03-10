# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

"""
Client implementation for CouchDB access. It allows you to manage a CouchDB
server, databases, documents and views. All objects mostly reflect python
objects for convenience. Server and Database objects for example, can be
used as easy as a dict.

Example:

    >>> from couchdbkit import Server
    >>> server = Server()
    >>> db = server.create_db('couchdbkit_test')
    >>> doc = { 'string': 'test', 'number': 4 }
    >>> db.save_doc(doc)
    >>> docid = doc['_id']
    >>> doc2 = db.get(docid)
    >>> doc['string']
    u'test'
    >>> del db[docid]
    >>> docid in db
    False
    >>> del server['simplecouchdb_test']

"""

import itertools
import tempfile
import time
import types
try:
    import simplejson as json
except ImportError:
    import json
    
from restkit import util

from couchdbkit.errors import ResourceNotFound, ResourceConflict, NoResultFound,\
MultipleResultsFound, BulkSaveError, InvalidAttachment
from couchdbkit.resource import CouchdbResource, encode_params, escape_docid,\
encode_attachments, couchdb_version

aliases = {
    'id': '_id',
    'rev': '_rev' 
}

class Uuids(CouchdbResource):
    
    def __init__(self, uri, max_uuids=1000, **client_opts):
        CouchdbResource.__init__(self, uri=uri, **client_opts)
        self._uuids = []
        self.max_uuids = max_uuids
        
    def next(self):
        if not self._uuids:
            self.fetch_uuids()
        self._uuids, res = self._uuids[:-1], self._uuids[-1]
        return res   
        
    def __iter__(self):
        return self
        
    def fetch_uuids(self):
        count = self.max_uuids - len(self._uuids)
        resp = self.get('/_uuids', count=count)
        self._uuids += resp.json_body['uuids']

class Server(CouchdbResource):
    """ Server object that allows you to access and manage a couchdb node.
    A Server object can be used like any `dict` object.
    """
    
    def info(self):
        """ return server info line"""
        resp = self.get()
        return resp.json_body
        
    def version(self):
        """ return server version as a tupple """
        version = self.info()['version']
        t = []
        for p in version.split("."):
            try:
                t.append(int(p))
            except ValueError:
                continue
        return tuple(t)
    
    def create_db(self, dbname):
        """ create a database 
        
        @param dbname: name of database
        @return: `couchdbkit.client.Database` instance
        """
        if "/" in dbname:
            dbname = util.url_quote(dbname, safe='')
        
        return Database(self._db_uri(dbname), create=True, **self.client_opts)
        
    def open_db(self, dbname):
        """ open a database
        
        @param dbname: name of database
        @return: `couchdbkit.client.Database` instance
        """
        if "/" in dbname:
            dbname = util.url_quote(dbname, safe='')
        return Database(self._db_uri(dbname), **self.client_opts)
        
    def open_or_create_db(self, dbname):
        """ Open or create a database
        
        @param dbname: name of database
        @return: `couchdbkit.client.Database` instance
        """
        if "/" in dbname:
            dbname = util.url_quote(dbname, safe='')
        return Database(self._db_uri(dbname), force_create=True, 
                    **self.client_opts)
        
    def delete_db(self, dbname):
        """ Delete a database """
        if "/" in dbname:
            dbname = util.url_quote(dbname, safe='')
            
        resp = self.delete(dbname)
        return resp.json_body
    
    def all_dbs(self):
        """ Get list of database names """
        return self.get('_all_dbs').json_body

    def uuids(self, count=1):
        """ get a list of uuids from the server """
        resp = self.get('/_uuids', count=count)
        return resp.json_body['uuids']
        
    def replicate(self, source, target, continuous=False):
        """
        simple handler for replication

        @param source: str, URI or dbname of the source
        @param target: str, URI or dbname of the target
        @param continuous: boolean, default is False, set the type of replication

        More info about replication here :
        http://wiki.apache.org/couchdb/Replication

        """
        resp = self.post('/_replicate', payload={
            "source": source,
            "target": target,
            "continuous": continuous
        })
        return resp.json_body
        
    def __getitem__(self, dbname):
        return self.open_db(dbname)
        
    def __delitem__(self, dbname):
        return self.delete_db(dbname)
        
    def __iter__(self):
        for dbname in self.all_dbs:
            yield Database(self._db_uri(dbname), **self.client_opts)
            
    def __contains__(self, dbname):
        try:
            self.head(dbname)
        except ResourceNotFound:
            return False
        return True
        
    def __len__(self):
        return len(self.all_dbs())
        
    def __nonzero__(self):
        return (len(self) > 0)
        
    def _db_uri(self, dbname):
        if dbname.startswith("/"):
            dbname = dbname[1:]
        return "/".join([self.uri, dbname])

class Database(CouchdbResource):
    """ Object that abstract access to a CouchDB database
    A Database object can act as a Dict object.
    """
    
    def __init__(self, uri, create=False, force_create=False, **client_opts):
        CouchdbResource.__init__(self, uri=uri, **client_opts)

        self.server_uri, self.dbname = uri.rsplit('/', 1)
        
        self.uuids = Uuids(self.server_uri)
        self.version = couchdb_version(self.server_uri)
        
        if self.uri.endswith("/"):
            self.uri = self.uri[:-1]
        
        # create the db
        if create:
            self.put()
        else:
            try:
                self.head()
            except ResourceNotFound:
                if not force_create:
                    raise
                self.put()
        
    def info(self):
        """
        Get database information

        @param _raw_json: return raw json instead deserializing it

        @return: dict
        """
        return self.get().json_body
        
    def all_docs(self, **params):
        """
        return all_docs
        """
        return self.view('_all_docs', **params)
            
    def open_doc(self, docid, wrapper=None, **params):
        """Open document from database

        Args:
        @param docid: str, document id to retrieve
        @param rev: if specified, allows you to retrieve
        a specific revision of document
        @param wrapper: callable. function that takes dict as a param.
        Used to wrap an object.
        @params params: Other params to pass to the uri (or headers)
        
        @return: dict, representation of CouchDB document as
         a dict.
        """
        resp = self.get(escape_docid(docid), **params)
        
        if wrapper is not None:
            if not callable(wrapper):
                raise TypeError("wrapper isn't a callable")
            return wrapper(resp.json_body)
        return resp.json_body
        
    def save_doc(self, doc, encode=True, force_update=False, **params):
        """ Save a document. It will use the `_id` member of the document
        or request a new uuid from CouchDB. IDs are attached to
        documents on the client side because POST has the curious property of
        being automatically retried by proxies in the event of network
        segmentation and lost responses. 

        @param doc: dict.  doc is updated
        with doc '_id' and '_rev' properties returned
        by CouchDB server when you save.
        @param force_update: boolean, if there is conlict, try to update
        with latest revision
        @param encode: Encode attachments if needed (depends on couchdb version)

        @return: new doc with updated revision an id
        """  
        if '_attachments' in doc and encode:
            doc['_attachments'] = encode_attachments(doc['_attachments'])
            
        if '_id' in doc:
            docid = escape_docid(doc['_id'])
            try:
                resp = self.put(docid, payload=json.dumps(doc), **params)
            except ResourceConflict:
                if not force_update:
                    raise
                rev = self.last_rev(doc['_id'])
                doc['_rev'] = rev
                resp = self.put(docid, payload=json.dumps(doc), **params)
        else:
            json_doc = json.dumps(doc)
            try:
                doc['_id'] = self.uuids.next()
                resp = self.put(doc['_id'], payload=json_doc, **params)
            except ResourceConflict:
                resp = self.post(payload=json_doc, **params)
            
        json_res = resp.json_body
        doc1 = {}
        for a, n in aliases.items():
            if a in json_res:
                doc1[n] = json_res[a]
        print doc
        doc.update(doc1)
        
    def last_rev(self, docid):
        """ Get last revision from docid (the '_rev' member)
        @param docid: str, undecoded document id.

        @return rev: str, the last revision of document.
        """
        r = self.head(escape_docid(docid))
        return r.headers['etag'].strip('"')
        
    def delete_doc(self, id_or_doc):
        """ Delete a document
        @param id_or_doc: docid string or document dict
        
        """
        if isinstance(id_or_doc, types.StringType):
            docid = id_or_doc
            resp = self.delete(escape_docid(id_or_doc), 
                        rev=self.last_rev(id_or_doc))    
        else:
            docid = id_or_doc.get('_id')
            if not docid:
                raise ValueError('Not valid doc to delete (no doc id)')
            rev = id_or_doc.get('_rev', self.last_rev(docid))
            resp = self.delete(escape_docid(docid), rev=rev)
        return resp.json_body
        
    def save_docs(self, docs, all_or_nothing=False, use_uuids=True):
        """ Bulk save. Modify Multiple Documents With a Single Request

        @param docs: list of docs
        @param use_uuids: add _id in doc who don't have it already set.
        @param all_or_nothing: In the case of a power failure, when the database 
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking, so the documents will


        @return doc lists updated with new revision or raise BulkSaveError 
        exception. You can access to doc created and docs in error as properties
        of this exception.
        """
            
        def is_id(doc):
            return '_id' in doc
            
        if use_uuids:
            noids = []
            for k, g in itertools.groupby(docs, is_id):
                if not k:
                    noids = list(g)
                    
            for doc in noids:
                nextid = self.uuids.next()
                if nextid:
                    doc['_id'] = nextid
                    
        payload = { "docs": docs }
        if all_or_nothing:
            payload["all-or-nothing"] = True
            
        # update docs
        res = self.post('/_bulk_docs', payload=json.dumps(payload))
            
        json_res = res.json_body
        errors = []
        for i, r in enumerate(json_res):
            if 'error' in r:
                errors.append(r)
            else:
                docs[i].update({'_id': r['id'], 
                                '_rev': r['rev']})
                                
        if errors:
            raise BulkSaveError(docs, errors)
            
    def delete_docs(self, docs, all_or_nothing=False, use_uuids=True):
        """ multiple doc delete."""
        for doc in docs:
            doc['_deleted'] = True
        return self.save_docs(docs, all_or_nothing=all_or_nothing, 
                            use_uuids=use_uuids)

    def fetch_attachment(self, id_or_doc, name, headers=None):
        """ get attachment in a document

        @param id_or_doc: str or dict, doc id or document dict
        @param name: name of attachment default: default result
        @param header: optionnal headers (like range)
        
        @return: `couchdbkit.resource.CouchDBResponse` object
        """
        if isinstance(id_or_doc, basestring):
            docid = id_or_doc
        else:
            docid = id_or_doc['_id']
      
        return self.get("%s/%s" % (escape_docid(docid), name), headers=headers)
        
    def put_attachment(self, doc, content=None, name=None, headers=None):
        """ Add attachement to a document. All attachments are streamed.

        @param doc: dict, document object
        @param content: string, iterator,  fileobj
        @param name: name or attachment (file name).
        @param headers: optionnal headers like `Content-Length` 
        or `Content-Type`

        @return: updated document object
        """
        headers = {}
        content = content or ""
            
        if name is None:
            if hasattr(content, "name"):
                name = content.name
            else:
                raise InvalidAttachment(
                            'You should provid a valid attachment name')
        name = util.url_quote(name, safe="")
        res = self.put("%s/%s" % (escape_docid(doc['_id']), name), 
                    payload=content, headers=headers, rev=doc['_rev'])
        json_res = res.json_body
        
        if 'ok' in json_res:
            return doc.update(self.open_doc(doc['_id']))
        return False
        
    def delete_attachment(self, doc, name):
        """ delete attachement to the document
        
        @param doc: dict, document object in python
        @param name: name of attachement

        @return: updated document object
        """
        name = util.url_quote(name, safe="")
        self.delete("%s/%s" % (escape_docid(doc['_id']), name), 
                        rev=doc['_rev']).json_body
        return doc.update(self.open_doc(doc['_id']))
         
    def view(self, view_name, wrapper=None, **params):
        """ get view results from database. viewname is generally
        a string like `designname/viewname". It return an ViewResults
        object on which you could iterate, list, ... . You could wrap
        results in wrapper function, a wrapper function take a row
        as argument. Wrapping could be also done by passing an Object
        in obj arguments. This Object should have a `wrap` method
        that work like a simple wrapper function.

        @param view_name, string could be '_all_docs', ,
        'designname/viewname' or a tuple. 
        @param wrapper: function used to wrap results
        @param params: params of the view 
        
        @return a View object
        """
        
        if isinstance(view_name, tuple):
            dname, vname = view_name
        else:
        
            if view_name.startswith('/'):
                view_name = view_name[1:]
                
            if view_name == '_all_docs':
                view_path = view_name
            else:
                view_name = view_name.split('/')
                dname = view_name.pop(0)
                vname = '/'.join(view_name)
                view_path = '_design/%s/_view/%s' % (dname, vname)
                
        view_uri = '%s/%s' % (self.uri, view_path)
        
        return View(view_uri, wrapper=wrapper, client_opts=self.client_opts,
                    params=params)

    def search( self, view_name, handler='_fti', wrapper=None, **params):
        """ Search. Return results from search. Use couchdb-lucene
        with its default settings by default."""
        
        uri = "%s/%s/%s" % (self.uri, handler, view_name)
        return View(uri, wrapper=wrapper, client_opts=self.client_opts,
                    params=params)
                   
    def copy_doc(self, id_or_doc, dest=None):
        """ copy an existing document to a new id. If dest is None, a new uuid
         will be requested
        
        @param doc: dict or string, document or document id
        @param dest: string or dict. if _rev is specified in dict 
        it will override the doc
        
        @return: new doc
        """
        if isinstance(id_or_doc, types.StringType):
            docid = id_or_doc
        else:
            if not '_id' in id_or_doc:
                raise KeyError('_id is required to copy a doc')
            docid = id_or_doc['_id']

        if dest is None:
            destid = destination = self.uuids.next()
        elif isinstance(dest, types.StringType):
            try:
                rev = self.last_rev(dest)
                destination = "%s?rev=%s" % (dest, rev)
            except ResourceNotFound:
                destination = dest
            destid = dest
        elif '_id' in dest and '_rev' in dest:
            destination = "%s?rev=%s" % (dest['_id'], dest['_rev'])
            destid = dest['_id']
        else:
            raise ValueError("invalid destination")    
          
        self.copy('/%s' % docid, headers={ 
                        "Destination": str(destination) })
        return self.open_doc(destid)
        
    def compact(self, dname=None):
        """ compact database
        @param dname: string, name of design doc. Usefull to
        compact a view.
        """
        path = "/_compact"
        if dname is not None:
            path = "%s/%s" % (path, escape_docid(dname))
        resp = self.post(path)
        return resp.json_body

    def view_cleanup(self):
        res = self.post('/_view_cleanup')
        return res.json_body
        
    def ensure_full_commit(self):
        """ commit all docs in memory """
        return self.post('_ensure_full_commit')

    def flush(self):
        """ Remove all docs from a database
        except design docs."""
        # save ddocs
        all_ddocs = self.all_docs(startkey="_design",
                            endkey="_design/"+u"\u9999",
                            include_docs=True)
        ddocs = []
        for ddoc in all_ddocs:
            ddoc['doc'].pop('_rev')
            ddocs.append(ddoc['doc'])

        # delete db
        self.delete()

        # we let a chance to the system to sync
        time.sleep(0.2)

        # recreate db + ddocs
        self.put()
        self.save_docs(ddocs)
        
    def doc_exist(self, docid):
        return docid in self
            
    def __getitem__(self, docid):
        return self.open_doc(docid)
        
    def __delitem__(self, docid):
        return self.delete_doc(docid)

    def __setitem__(self, docid, doc):
        doc['_id'] = docid
        self.save_doc(doc)
        
    def __contains__(self, docid):
        try:
            self.head(escape_docid(docid))
        except ResourceNotFound:
            return False
        return True
        
    def __len__(self):
        return self.info()['doc_count']
        
    def __nonzero__(self):
        return (len(self) > 0)

 
class View(CouchdbResource):
    
    cache=True
    
    def __init__(self, uri, wrapper=None, client_opts=None, params=None):
        self.params = params or {}
        client_opts = client_opts or {}
        self.wrapper = wrapper
        self._result_cache = None        
        CouchdbResource.__init__(self, uri=uri, **client_opts)
        
        
    def fetch(self, extra_params=None, nocache=False):
        extra_params = extra_params or {}
        params = self.params.copy()
        params.update(extra_params)
        
        if 'keys' in params:
            keys = params.pop('keys')
            payload = json.dumps({'keys': keys})
            resp = self.post(payload=payload, **encode_params(params))
        else:
            resp = self.get(**encode_params(params))
            
        json_res = resp.json_body
        if self.cache and not nocache:
            self._result_cache = json_res
        return json_res
        
    def maybe_fetch(self):
        if self.cache and self._result_cache is not None:
            return self._result_cache
        return self.fetch()
             
    def rows(self):
        """ return list of all results """
        return list(self)
        
    def count(self):
        """ return number of results"""
        results = self.maybe_fetch()
        return len(results.get('rows', []))
        
    def first(self):
        """
        Return the first result of this query or None if the result doesn’t contain any row.

        This results in an execution of the underlying query.
        """
        try:
            return self.rows()[0]
        except IndexError:
            return None
            
    def one(self, except_all=False):
        """
        Return exactly one result or raise an exception.


        Raises `couchdbkit.exceptions.MultipleResultsFound` if multiple rows are returned.
        If except_all is True, raises `couchdbkit.exceptions.NoResultFound`
        if the query selects no rows.

        This results in an execution of the underlying query.
        """

        length = len(self)
        if length > 1:
            raise MultipleResultsFound("%s results found." % length)

        result = self.first()
        if result is None and except_all:
            raise NoResultFound()
        return result

    def __getattr__(self, key):
        try:
            getattr(super(View, self), key)
        except AttributeError:
            results = self.maybe_fetch()
            if key in results:
                return results[key]
            raise

    def __iter__(self):
        results = self.maybe_fetch()
        rows = results.get('rows', [])

        for row in rows:
            if self.wrapper is None:
                yield row
            else:
                yield self.wrapper(row)
            
    def __getitem__(self, key):
        params = self.params.copy()
        if type(key) is slice:
            if key.start is not None:
                params['startkey'] = key.start
            if key.stop is not None:
                params['endkey'] = key.stop
        elif isinstance(key, (list, tuple,)):
            params['keys'] = key
        else:
            params['key'] = key

        return type(self)(self.uri, wrapper=self.wrapper, 
                        client_opts=self.client_opts, params=params)

    def __len__(self):
        return self.count()

    def __nonzero__(self):
        return bool(len(self))