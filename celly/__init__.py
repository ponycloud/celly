#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Celly', 'UserError', 'DataError', 'AccessError', 'PathError',
           'ConflictError', 'RequestError', 'MethodError', 'PatchError']

from httplib2 import Http
from urllib import quote
from os.path import dirname

from simplejson import loads, dumps
import re

class RequestError(Exception):
    def __init__(self, message, **kw):
        super(RequestError, self).__init__(message)

        self._data = kw
        for k, v in kw.iteritems():
            if not k.startswith('_'):
                setattr(self, k, v)

    @classmethod
    def from_response(cls, status, data):
        code = int(status['status'])

        if not isinstance(data, dict):
            data = {}

        if code == 400:
            if 'invalid-data' == data['error']:
                return DataError(code=code, **data)
            if 'invalid-patch' == data['error']:
                return PatchError(code=code, **data)
            return UserError(code=code, **data)

        if code == 403:
            return AccessError(code=code, **data)

        if code == 404:
            return PathError(code=code, **data)

        if code == 405:
            return MethodError(code=code, message='method not allowed')

        if code == 409:
            return ConflictError(code=code, **data)

        if 'message' in data:
            return cls(code=code, **data)
        return cls(code=code, message='request failed')

    def __str__(self):
        lines = [self.message, '  Info:']
        for k, v in sorted(self._data.items()):
            lines.append('    %s: %r' % (k, v))
        return '\n'.join(lines) + '\n'

class MethodError(RequestError):
    pass

class UserError(RequestError):
    pass

class AccessError(RequestError):
    pass

class DataError(RequestError):
    pass

class ConflictError(DataError):
    pass

class PathError(DataError):
    pass

class PatchError(DataError):
    pass

class CollectionProxy(object):
    """Remote collection proxy."""

    def __init__(self, celly, uri, schema):
        self.celly = celly
        self.uri = uri
        self.schema = schema

    def __iter__(self):
        return iter(self.list)

    def __len__(self):
        return len(self.list)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.list[key]

        child_uri = '%s%s' % (self.uri, quote(key, ''))
        return EntityProxy(self.celly, child_uri, self.schema)

    def _get_key(self, item):
        if 'desired' in item:
            return item['desired'][self.schema['pkey']]
        return item['current'][self.schema['pkey']]

    @property
    def list(self):
        out = []
        for key, value in self.celly.request(self.uri).iteritems():
            child_uri = '%s%s' % (self.uri, quote(key, ''))
            out.append(EntityProxy(self.celly, child_uri, self.schema))
        return out

    def post(self, data):
        return self.celly.request(self.uri, 'POST', dumps(data))

    def patch(self, ops):
        return self.celly.request(self.uri, 'PATCH', dumps(ops))

    def merge(self, value):
        return self.patch([{'op': 'x-merge', 'path': '/', 'value': value}])

    def __repr__(self):
        return '<CollectionProxy %s>' % self.uri


class EntityProxy(object):
    """Remote entity proxy."""

    def __init__(self, celly, uri, schema):
        self.celly = celly
        self.uri = uri
        self.schema = schema

        for name, child in self.schema['children'].iteritems():
            uri = '%s/%s/' % (self.uri, quote(name, ''))
            name = name.replace('-', '_')
            setattr(self, name, CollectionProxy(self.celly, uri, child))

    @property
    def desired(self):
        return self.celly.request(self.uri).get('desired')

    @property
    def current(self):
        return self.celly.request(self.uri).get('current')

    def delete(self):
        return self.celly.request(self.uri, 'DELETE')

    def patch(self, ops):
        return self.celly.request(self.uri, 'PATCH', dumps(ops))

    def merge(self, value):
        return self.patch([{'op': 'x-merge', 'path': '/', 'value': value}])

    def __repr__(self):
        return '<EntityProxy %s>' % self.uri


class Celly(object):
    """Ponycloud RESTful API client."""

    def __init__(self, base_uri='http://127.0.0.1:9860/v1', auth=None):
        """
        Queries the API schema and constructs client accordingly.

        :param base_uri:  Address of the Sparkle API.
        :param auth:      Optional authentication data.
                          Either a string with bearer token to be used
                          directly or a tuple with user name and password
                          for basic authentication.
        """

        self.uri = base_uri
        self.http = Http()
        self.headers = {}

        if isinstance(auth, basestring):
            self.headers['Authorization'] = 'Token ' + auth
        else:
            base64 = ':'.join(auth).encode('base64')
            self.headers['Authorization'] = 'Basic ' + base64

        for name, child in self.schema['children'].iteritems():
            uri = '%s/%s/' % (base_uri, quote(name, ''))
            name = name.replace('-', '_')
            setattr(self, name, CollectionProxy(self, uri, child))

    def request(self, uri, method='GET', body=None, headers={}):
        bh = self.headers.copy()
        bh.update(headers)

        status, data = \
                self.http.request(uri, method=method, body=body, headers=bh)

        if status.get('content-type') == 'application/json':
            data = loads(data)

        if int(status['status']) == 200:
            return data

        raise RequestError.from_response(status, data)


    @property
    def schema(self):
        if not hasattr(self, '_schema'):
            self._schema = {
                'children': self.request('%s/schema' % (self.uri,))
            }
        return self._schema


# vim:set sw=4 ts=4 et:
