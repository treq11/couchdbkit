# -*- coding: utf-8 -
#
# This file is part of couchdbkit released under the MIT license. 
# See the NOTICE for more information.

""" module that provides a Document object that allows you
to map CouchDB document in Python statically, dynamically or both
"""

import datetime
import decimal
import re
import warnings

from couchdbkit.client import Database
from couchdbkit.schema import properties as p
from couchdbkit.schema.properties import value_to_python, \
convert_property, MAP_TYPES_PROPERTIES, ALLOWED_PROPERTY_TYPES, \
LazyDict, LazyList, value_to_json
from couchdbkit.errors import *
from couchdbkit.resource import ResourceNotFound


__all__ = ['ReservedWordError', 'ALLOWED_PROPERTY_TYPES', 'DocumentSchema',
        'SchemaProperties', 'DocumentBase', 'QueryMixin', 'AttachmentMixin',
        'Document', 'StaticDocument', 'valid_id']

_RESERVED_WORDS = ['_id', '_rev', '$schema']

_NODOC_WORDS = ['doc_type']

def check_reserved_words(attr_name):
    if attr_name in _RESERVED_WORDS:
        raise ReservedWordError(
            "Cannot define property using reserved word '%(attr_name)s'." %
            locals())

def valid_id(value):
    if isinstance(value, basestring) and not value.startswith('_'):
        return value
    raise TypeError('id "%s" is invalid' % value)

class SchemaProperties(type):

    def __new__(cls, name, bases, attrs):
        # init properties
        properties = {}
        defined = set()
        for base in bases:
            if hasattr(base, '_properties'):
                property_keys = base._properties.keys()
                duplicate_properties = defined.intersection(property_keys)
                if duplicate_properties:
                    raise DuplicatePropertyError(
                        'Duplicate properties in base class %s already defined: %s' % (base.__name__, list(duplicate_properties)))
                defined.update(property_keys)
                properties.update(base._properties)

        doc_type = attrs.get('doc_type', False)
        if not doc_type:
            doc_type = name
        else:
            del attrs['doc_type']

        attrs['_doc_type'] = doc_type

        for attr_name, attr in attrs.items():
            # map properties
            if isinstance(attr, p.Property):
                check_reserved_words(attr_name)
                if attr_name in defined:
                    raise DuplicatePropertyError('Duplicate property: %s' % attr_name)
                properties[attr_name] = attr
                attr.__property_config__(cls, attr_name)
            # python types
            elif type(attr) in MAP_TYPES_PROPERTIES and \
                    not attr_name.startswith('_') and \
                    attr_name not in _NODOC_WORDS:
                check_reserved_words(attr_name)
                if attr_name in defined:
                    raise DuplicatePropertyError('Duplicate property: %s' % attr_name)
                prop = MAP_TYPES_PROPERTIES[type(attr)](default=attr)
                properties[attr_name] = prop
                prop.__property_config__(cls, attr_name)
                attrs[attr_name] = prop

        attrs['_properties'] = properties
        return type.__new__(cls, name, bases, attrs)


class DocumentSchema(object):
    __metaclass__ = SchemaProperties

    _dynamic_properties = None
    _allow_dynamic_properties = True
    _doc = None
    _db = None

    def __init__(self, _d=None, **properties):
        self._dynamic_properties = {}
        self._doc = {}

        if _d is not None:
            if not isinstance(_d, dict):
                raise TypeError('d should be a dict')
            properties.update(_d)

        doc_type = getattr(self, '_doc_type', self.__class__.__name__)
        self._doc['doc_type'] = doc_type

        for prop in self._properties.values():
            if prop.name in properties:
                value = properties.pop(prop.name)
                if value is None:
                    value = prop.default_value()
            else:
                value = prop.default_value()
            prop.__property_init__(self, value)

        _dynamic_properties = properties.copy()
        for attr_name, value in _dynamic_properties.iteritems():
            if attr_name not in self._properties \
                    and value is not None:
                if isinstance(value, p.Property):
                    value.__property_config__(self, attr_name)
                    value.__property_init__(self, value.default_value())
                elif isinstance(value, DocumentSchema):
                    from couchdbkit.schema import SchemaProperty
                    value = SchemaProperty(value)
                    value.__property_config__(self, attr_name)
                    value.__property_init__(self, value.default_value())


                setattr(self, attr_name, value)
                # remove the kwargs to speed stuff
                del properties[attr_name]

    def dynamic_properties(self):
        """ get dict of dynamic properties """
        if self._dynamic_properties is None:
            return {}
        return self._dynamic_properties.copy()

    def properties(self):
        """ get dict of defined properties """
        return self._properties.copy()

    def all_properties(self):
        """ get all properties.
        Generally we just need to use keys"""
        all_properties = self._properties.copy()
        all_properties.update(self.dynamic_properties())
        return all_properties

    def to_json(self):
        if self._doc.get('doc_type') is None:
            doc_type = getattr(self, '_doc_type', self.__class__.__name__)
            self._doc['doc_type'] = doc_type
        return self._doc

    #TODO: add a way to maintain custom dynamic properties
    def __setattr__(self, key, value):
        """
        override __setattr__ . If value is in dir, we just use setattr.
        If value is not known (dynamic) we test if type and name of value
        is supported (in ALLOWED_PROPERTY_TYPES, Property instance and not
        start with '_') a,d add it to `_dynamic_properties` dict. If value is
        a list or a dict we use LazyList and LazyDict to maintain in the value.
        """

        if key == "_id" and valid_id(value):
            self._doc['_id'] = value
        else:
            check_reserved_words(key)
            if not hasattr( self, key ) and not self._allow_dynamic_properties:
                raise AttributeError("%s is not defined in schema (not a valid property)" % key)

            elif not key.startswith('_') and \
                    key not in self.properties() and \
                    key not in dir(self):
                if type(value) not in ALLOWED_PROPERTY_TYPES and \
                        not isinstance(value, (p.Property,)):
                    raise TypeError("Document Schema cannot accept values of type '%s'." %
                            type(value).__name__)

                if self._dynamic_properties is None:
                    self._dynamic_properties = {}

                if isinstance(value, dict):
                    if key not in self._doc or not value:
                        self._doc[key] = {}
                    elif not isinstance(self._doc[key], dict):
                        self._doc[key] = {}
                    value = LazyDict(self._doc[key], init_vals=value)
                elif isinstance(value, list):
                    if key not in self._doc or not value:
                        self._doc[key] = []
                    elif not isinstance(self._doc[key], list):
                        self._doc[key] = []
                    value = LazyList(self._doc[key], init_vals=value)

                self._dynamic_properties[key] = value

                if not isinstance(value, (p.Property,)) and \
                        not isinstance(value, dict) and \
                        not isinstance(value, list):
                    if callable(value):
                        value = value()
                    self._doc[key] = convert_property(value)
            else:
                object.__setattr__(self, key, value)

    def __delattr__(self, key):
        """ delete property
        """
        if key in self._doc:
            del self._doc[key]

        if self._dynamic_properties and key in self._dynamic_properties:
            del self._dynamic_properties[key]
        else:
            object.__delattr__(self, key)

    def __getattr__(self, key):
        """ get property value
        """
        if self._dynamic_properties and key in self._dynamic_properties:
            return self._dynamic_properties[key]
        elif key  in ('_id', '_rev', '_attachments'):
            return self._doc.get(key)
        return getattr(super(DocumentSchema, self), key)

    def __getitem__(self, key):
        """ get property value
        """
        try:
            attr = getattr(self, key)
            if callable(attr):
                raise AttributeError
            return attr
        except AttributeError, e:
            if key in self._doc:
                return self._doc[key]
            raise

    def __setitem__(self, key, value):
        """ add a property
        """
        setattr(self, key, value)


    def __delitem__(self, key):
        """ delete a property
        """
        try:
            delattr(self, key)
        except AttributeError, e:
            raise KeyError, e


    def __contains__(self, key):
        """ does object contain this propery ?

        @param key: name of property

        @return: True if key exist.
        """
        if key in self.all_properties():
            return True
        elif key in self._doc:
            return True
        return False

    def __iter__(self):
        """ iter document instance properties
        """
        for k in self.all_properties().keys():
            yield k, self[k]
        raise StopIteration

    iteritems = __iter__

    def items(self):
        """ return list of items
        """
        return [(k, self[k]) for k in self.all_properties().keys()]


    def __len__(self):
        """ get number of properties
        """
        return len(self._doc or ())

    def __getstate__(self):
        """ let pickle play with us """
        obj_dict = self.__dict__.copy()
        return obj_dict

    @classmethod
    def wrap(cls, data):
        """ wrap `data` dict in object properties """
        instance = cls()
        instance._doc = data
        for prop in instance._properties.values():
            if prop.name in data:
                value = data[prop.name]
                if value is not None:
                    value = prop.to_python(value)
                else:
                    value = prop.default_value()
            else:
                value = prop.default_value()
            prop.__property_init__(instance, value)

        if cls._allow_dynamic_properties:
            for attr_name, value in data.iteritems():
                if attr_name in instance.properties():
                    continue
                if value is None:
                    continue
                elif attr_name.startswith('_'):
                    continue
                elif attr_name == 'doc_type':
                    continue
                else:
                    value = value_to_python(value)
                    setattr(instance, attr_name, value)
        return instance

    def validate(self, required=True):
        """ validate a document """
        for attr_name, value in self._doc.items():
            if attr_name in self._properties:
                self._properties[attr_name].validate(
                        getattr(self, attr_name), required=required)
        return True

    def clone(self, **kwargs):
        """ clone a document """
        for prop_name in self._properties.keys():
            try:
                kwargs[prop_name] = self._doc[prop_name]
            except KeyError:
                pass

        kwargs.update(self._dynamic_properties)
        obj = type(self)(**kwargs)
        obj._doc = self._doc

        return obj

    @classmethod
    def build(cls, **kwargs):
        """ build a new instance from this document object. """
        obj = cls()
        properties = {}
        for attr_name, attr in kwargs.items():
            if isinstance(attr, (p.Property,)):
                properties[attr_name] = attr
                attr.__property_config__(cls, attr_name)
            elif type(attr) in MAP_TYPES_PROPERTIES and \
                    not attr_name.startswith('_') and \
                    attr_name not in _NODOC_WORDS:
                check_reserved_words(attr_name)

                prop = MAP_TYPES_PROPERTIES[type(attr)](default=attr)
                properties[attr_name] = prop
                prop.__property_config__(cls, attr_name)
                attrs[attr_name] = prop
        return type('AnonymousSchema', (cls,), properties)

class DocumentBase(DocumentSchema):
    """ Base Document object that map a CouchDB Document.
    It allow you to statically map a document by
    providing fields like you do with any ORM or
    dynamically. Ie unknown fields are loaded as
    object property that you can edit, datetime in
    iso3339 format are automatically translated in
    python types (date, time & datetime) and decimal too.

    Example of documentass

    .. code-block:: python

        from couchdbkit.schema import *
        class MyDocument(Document):
            mystring = StringProperty()
            myotherstring = unicode() # just use python types


    Document fields can be accessed as property or
    key of dict. These are similar : ``value = instance.key or value = instance['key'].``

    To delete a property simply do ``del instance[key'] or delattr(instance, key)``
    """
    _db = None

    def __init__(self, _d=None, **kwargs):
        docid = None
        if '_id' in kwargs:
            docid = kwargs.pop('_id')
        DocumentSchema.__init__(self, _d, **kwargs)
        if docid is not None:
            self._doc['_id'] = valid_id(docid)

    @classmethod
    def set_db(cls, db):
        """ Set document db"""
        cls._db = db

    @classmethod
    def get_db(cls):
        """ get document db"""
        db = getattr(cls, '_db', None)
        if db is None:
            raise TypeError("doc database required to save document")
        return db

    def save(self, **params):
        """ Save document in database.

        @params db: couchdbkit.core.Database instance
        """
        self.validate()
        if self._db is None:
            raise TypeError("doc database required to save document")

        doc = self.to_json()
        self._db.save_doc(doc, **params)
        self._doc = doc
    store = save

    @classmethod
    def bulk_save(cls, docs, use_uuids=True, all_or_nothing=False):
        """ Save multiple documents in database.

        @params docs: list of couchdbkit.schema.Document instance
        @param use_uuids: add _id in doc who don't have it already set.
        @param all_or_nothing: In the case of a power failure, when the database
        restarts either all the changes will have been saved or none of them.
        However, it does not do conflict checking, so the documents will
        be committed even if this creates conflicts.

        """
        if cls._db is None:
            raise TypeError("doc database required to save document")
        docs_to_save= [doc.to_json() for doc in docs \
                            if doc._doc_type == cls._doc_type]
        if not len(docs_to_save) == len(docs):
            raise ValueError("one of your documents does not have the correct type")
        cls._db.save_docs(docs_to_save, all_or_nothing=all_or_nothing, 
                    use_uuids=use_uuids)
        [cls.wrap(doc) for doc in docs_to_save]         
        

    @classmethod
    def get(cls, docid, db=None, dynamic_properties=True, **params):
        """ get document with `docid`
        """
        if db is not None:
            cls._db = db
        cls._allow_dynamic_properties = dynamic_properties
        if cls._db is None:
            raise TypeError("doc database required to save document")
        return cls._db.open_doc(docid, wrapper=cls.wrap, **params)
    
    @classmethod
    def get_or_create(cls, docid=None, db=None, dynamic_properties=True, 
                    **params):
        """ get  or create document with `docid` """
        if db is not None:
            cls._db = db

        cls._allow_dynamic_properties = dynamic_properties

        if cls._db is None:
            raise TypeError("doc database required to save document")

        if docid is None:
            obj = cls()
            obj.save(**params)
            return obj
            
        try:
            return cls._db.open_doc(docid, wrapper=cls.wrap, **params)
        except ResourceNotFound:
            obj = cls()
            obj._id = docid
            obj.save(**params)
            return obj

    new_document = property(lambda self: self._doc.get('_rev') is None)

    def delete(self):
        """ Delete document from the database.
        @params db: couchdbkit.core.Database instance
        """
        if self._db is None:
            raise TypeError("doc database required to save document")

        if self.new_document:
            raise TypeError("the document is not saved")

        self._db.delete_doc(self._id)

        # reinit document
        del self._doc['_id']
        del self._doc['_rev']

class AttachmentMixin(object):
    """
    mixin to manage doc attachments.

    """

    def put_attachment(self, content, name=None, headers=None):
        """ Add attachement to a document.

        @param content: string or :obj:`File` object.
        @param name: name or attachment (file name).
        @param headers: optionnal headers like `Content-Length` 
        or `Content-Type`

        @return: bool, True if everything was ok.
        """
        if not hasattr(self, '_db'):
            raise TypeError("doc database required to save document")
            
        return self.__class__._db.put_attachment(self._doc, 
                            content, name=name, headers=headers)

    def delete_attachment(self, name):
        """ delete document attachment

        @param name: name of attachement

        @return: dict, withm member ok setto True if delete was ok.
        """
        if not hasattr(self, '_db'):
            raise TypeError("doc database required to save document")
        return self.__class__._db.delete_attachment(self._doc, name)

    def fetch_attachment(self, name, headers=None):
        """ get attachment in a adocument

        @param name: name of attachment default: default result
        @param headers: optional headers (like range)

        @return: `couchdbkit.resource.CouchdbResponse` instance
        """
        if not hasattr(self, '_db'):
            raise TypeError("doc database required to save document")
        return self.__class__._db.fetch_attachment(self._doc, name,
                                            headers=headers)


class QueryMixin(object):
    """ Mixin that add query methods """

    @classmethod
    def view(cls, view_name, wrapper=None, dynamic_properties=True,
        wrap_doc=True, **params):
        """ Get documents associated view a view.
        Results of view are automatically wrapped
        to Document object.

        @params view_name: str, name of view
        @params wrapper: override default wrapper by your own
        @dynamic_properties: do we handle properties which aren't in
        the schema ? Default is True.
        @wrap_doc: If True, if a doc is present in the row it will be
        used for wrapping. Default is True.
        @params params:  params of view

        @return: :class:`simplecouchdb.core.ViewResults` instance. All
        results are wrapped to current document instance.
        """
        
        def default_wrapper(row):
            data = row.get('value')
            docid = row.get('id')
            doc = row.get('doc')
            if doc is not None and wrap_doc:
                cls._allow_dynamic_properties = dynamic_properties
                return cls.wrap(doc)
            elif not data or data is None:
                return row
            elif not isinstance(data, dict) or not docid:
                return row
            else:
                data['_id'] = docid
                if 'rev' in data:
                    data['_rev'] = data.pop('rev')
                cls._allow_dynamic_properties = dynamic_properties
                return cls.wrap(data)

        if wrapper is None:
            wrapper = default_wrapper

        if not wrapper:
            wrapper = None
        elif not callable(wrapper):
            raise TypeError("wrapper is not a callable")

        db = cls.get_db()
        return db.view(view_name, wrapper=wrapper, **params)


class Document(DocumentBase, QueryMixin, AttachmentMixin):
    """
    Full featured document object implementing the following :

    :class:`QueryMixin` for view & temp_view that wrap results to this object
    :class `AttachmentMixin` for attachments function
    """

class StaticDocument(Document):
    """
    Shorthand for a document that disallow dynamic properties.
    """
    _allow_dynamic_properties = False
