# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

"""
All exceptions used in couchdbkit.
"""

from restkit import ResourceError

class ResourceNotFound(ResourceError):
    """ raised when a resource not found on CouchDB"""
   
class ResourceConflict(ResourceError):
    """ raised when a conflict occured"""

class PreconditionFailed(ResourceError):
    """ precondition failed error """    
    
class RequestFailed(Exception): 
    """ raised when an http error occurs"""
    
class Unauthorized(Exception):
    """ raised when not authorized to access to CouchDB"""

class BulkSaveError(Exception):
    """ error raised when therer are conflicts in bulk save"""
    
    def __init__(self, docs, errors):
        Exception.__init__(self)
        self.docs = docs
        self.errors = errors

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

class ViewServerError(Exception):
    """ exception raised by view server"""
