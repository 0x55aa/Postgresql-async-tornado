#!/usr/bin/env python
# coding: utf-8
from __future__ import absolute_import, division, with_statement

import logging
import functools

try:
    import psycopg2
except ImportError:
    raise
from psycopg2.pool import SimpleConnectionPool
from tornado import gen
from tornado.ioloop import IOLoop


version = "0.1"
version_info = (0, 1, 0, 0)


class Poller(object):
    """"""
    def __init__(self, connection, callbacks=()):
        self._ioloop = IOLoop.instance()
        self._conn = connection
        self._callbacks = callbacks

    def _update_handler(self):
        state = self._conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            for callback in self._callbacks:
                callback()
        elif state == psycopg2.extensions.POLL_READ:
            self._ioloop.add_handler(
                self._conn.fileno(), self._io_callback, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.add_handler(
                self._conn.fileno(), self._io_callback, IOLoop.WRITE)

    def _io_callback(self, *args):
        self._ioloop.remove_handler(self._conn.fileno())
        self._update_handler()


class Connection(object):
    """"""
    def __init__(self, database, host=None, port=None, user=None,
                 password=None, client_encoding="utf8",
                 minconn=1, maxconn=5,
                 **kwargs):

        self.host = "%s:%s" % (host, port)

        _db_args = dict(
            async=True,
            database=database,
            client_encoding=client_encoding,
            **kwargs
        )
        if host is not None:
            _db_args["host"] = host
        if port is not None:
            _db_args["port"] = port
        if user is not None:
            _db_args["user"] = user
        if password is not None:
            _db_args["password"] = password

        try:
            self._pool = SimpleConnectionPool(
                minconn=minconn, maxconn=maxconn, **_db_args)
        except Exception:
            logging.error("Cannot connect to PostgreSQL on %s", self.host,
                          exc_info=True)

    def __del__(self):
        self._pool.closeall()

    def _connect(self, callback=None):
        """Get an existing database connection."""
        conn = self._pool.getconn()

        callback = functools.partial(callback, conn)
        Poller(conn, (callback, ))._update_handler()

    @gen.coroutine
    def _cursor(self):
        conn = yield gen.Task(self._connect)
        cursor = conn.cursor()
        raise gen.Return(cursor)

    def putconn(self, conn, close=False):
        self._pool.putconn(conn, close=close)

    @gen.coroutine
    def query(self, query, parameters=()):
        """Returns a row list for the given query and parameters."""
        cursor = yield self._cursor()
        try:
            yield gen.Task(self._execute, cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            raise gen.Return([Row(zip(column_names, row)) for row in cursor])
        finally:
            self.putconn(cursor.connection)
            cursor.close()

    @gen.coroutine
    def get(self, query, parameters=()):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = yield self.query(query, parameters)
        if not rows:
            raise gen.Return(None)
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            raise gen.Return(rows[0])

    @gen.coroutine
    def execute(self, query, parameters=()):
        """Executes the given query."""
        cursor = yield self._cursor()
        try:
            yield gen.Task(self._execute, cursor, query, parameters)
        finally:
            self.putconn(cursor.connection)
            cursor.close()

    def _execute(self, cursor, query, parameters, callback=None):
        if not isinstance(parameters, (tuple, list)):
            raise

        try:
            cursor.execute(query, parameters)

            Poller(cursor.connection, (callback,))._update_handler()
        except psycopg2.OperationalError:
            logging.error("Error connecting to PostgreSQL on %s", self.host)
            self.putconn(cursor.connection, close=True)
            # raise gen.Return([])


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
