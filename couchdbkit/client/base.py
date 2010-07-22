# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

import urlparse

from restkit.client import HttpConnection, HttpResponse
from restkit.filters.basicauth import BasicAuth
from restkit.filters.oauth2 import OAuthFilter
from restkit.pool.simple import SimplePool
from restkit.util import make_uri
from restkit.util import oauth2 as oauth

try:
    import json
except ImportError:
    from simplejson import json
    
from couchdbkit import __version__
from couchdbkit.exceptions import ResourceNotFound, ResourceConflict, \
Unauthorized, RequestFailed

USER_AGENT = 'couchdbkit/%s' % __version__

_default_pool = None
def default_pool():
    global _default_pool
    if _default_pool is None:
        _default_pool = SimplePool()
    return _default_pool

class CouchdbResponse(HttpResponse):
    
    def body_json(self):
        body = self.body_string()
        try:
            return json.loads(body)
        except ValueError:
            return body
            
            
class CouchdbResource(object):
    
    response_class = CouchdbResponse
    
    def __init__(self, uri="http://127.0.0.1:5984", oauth_key=None, 
            oauth_secret=None, **extra):
        """ Init CouchdbResiource
        
        :param uri: resource uri, str
        :param oauth_key: oauth consumer key, str
        :param oauth_secret: oauth consumer secret key, str
        :param extra: dict, extra argguments passed to restkit http client.
        """
        self.extra = extra or {}
        filters = self.extra.get('filters') or []
        
        # set default response_class
        if self.response_class is not None:
            self.extra['response_class'] = self.response_class
            
        # set default pool if needed
        if not 'pool_instance' in self.extra:
            self.extra['pool_instance'] = default_pool()
        
        # manage auth
        url_parsed = urlparse.urlparse(uri)
        if url_parsed.username:
            password = u.password or ""
            filters.append(BasicAuth(url_parsed.username, password))
            uri = urlparse.urlunparse((u.scheme, u.netloc.split("@")[-1],
                u.path, u.params, u.query, u.fragment))    
        elif oauth is not None:
            consumer = oauth.Consumer(key=oauth_key, secret=oauth_secret)
            filters.append(OAuthFilter('*', consumer))
    
        self.extra['filters'] = filters
        self.uri = uri
    
    def make_params(self, params):
        params = params or {}
        for name, value in params.items():
            if value is None:
                continue

            if name in ('key', 'startkey', 'endkey') \
                    or not isinstance(value, basestring):
                value = json.dumps(value)
                params[name] = value
        return params
        
    def make_headers(self, headers):
        headers = headers or {}
        headers.setdefault('Accept', 'application/json')
        headers.setdefault('User-Agent', USER_AGENT)
        
    def unauthorized(self, response):
        return True
    
    def request(self, method='GET', path=None, payload=None, headers=None,
        **params):
        
        while True:
            uri = make_uri(self.connection_uri, path, 
                    **self.make_params(params))
            # make request
            http = HttpConnection(**self.extra)
            resp = http.request(uri, method=method, body=payload, 
                        headers=self.make_headers(headers))
                        
            if resp is None:
                # race condition
                raise ValueError("Unkown error: response object is known")
                
            if resp.status_int >= 400:
                if resp.status_int == 404:
                    raise ResourceNotFound(resp.body_string(),
                                response=resp)
                elif resp.status_int == 412:
                    raise ResourceConflict(resp.body_string,
                                response=resp)
                elif resp.status_int in (401, 403):
                    if self.unauthorized(resp):
                        raise Unauthorized(response.body_string(), 
                                http_code=response.status_int, 
                                response=response)
                else:
                    raise RequestFailed(resp.body_string(), 
                        http_code=resp.status_int, response=resp)
            else:
                break
        return resp    
        
        