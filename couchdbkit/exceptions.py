# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

from restkit.errors import ResourceErro

"""
All exceptions used in couchdbkit.
"""

class InvalidAttachment(Exception):
    """ raised when an attachment is invalid """

class DuplicatePropertyError(Exception):
    """ exception raised when there is a duplicate 
    property in a model """

class BadValueError(Exception):
    """ exception raised when a value can't be validated 
    or is required """

class MultipleResultsFound(Exception):
    """ exception raised when more than one object is
    returned by the get_by method"""
    
class NoResultFound(Exception):
    """ exception returned when no results are found """
    
class ReservedWordError(Exception):
    """ exception raised when a reserved word
    is used in Document schema """
    
class DocsPathNotFound(Exception):
    """ exception raised when path given for docs isn't found """
    
class BulkSaveError(Exception):
    """ exception raised when bulk save contain errors.
    error are saved in `errors` property.
    """
    def __init__(self, errors, *args):
        self.errors = errors

class ViewServerError(Exception):
    """ exception raised by view server"""
    
    
# resource exceptions

class ResourceNotFound(ResourceError):
    """Exception raised when no resource was found at the given url. 
    """
    status_int = 404

class ResourceConflict(ResourceError):
    """Exception raised when a database or a document already exists"""
    status_int = 412
    
class Unauthorized(ResourceError):
    """Exception raised when an authorization is required to access to
    the resource specified.
    """

class RequestFailed(ResourceError):
    """Exception raised when an unexpected HTTP error is received in response
    to a request.
    

    The request failed, meaning the remote HTTP server returned a code 
    other than success, unauthorized, or NotFound.

    The exception message attempts to extract the error

    You can get the status code by e.http_code, or see anything about the 
    response via e.response. For example, the entire result body (which is 
    probably an HTML error page) is e.response.body.
