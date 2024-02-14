# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# design:
# A threaded worker accepts connections in the main loop, accepted
# connections are added to the thread pool as a connection job.
# Keepalive connections are put back in the loop waiting for an event.
# If no event happen after the keep alive timeout, the connection is
# closed.
# pylint: disable=no-else-break

from concurrent import futures
import errno
import os
import queue
import selectors
import socket
import ssl
import sys
import time
from collections import deque
from datetime import datetime
from functools import partial

from . import base
from .. import http
from .. import util
from .. import sock
from ..http import wsgi


class TConn(object):

    def __init__(self, cfg, sock, client, server):
        self.cfg = cfg
        self.sock = sock
        self.client = client
        self.server = server

        self.timeout = None
        self.parser = None
        self.initialized = False

        # set the socket to non blocking
        self.sock.setblocking(False)

    def init(self):
        self.initialized = True

        if self.parser is None:
            # wrap the socket if needed
            if self.cfg.is_ssl:
                self.sock = sock.ssl_wrap_socket(self.sock, self.cfg)

            # initialize the parser
            self.parser = http.RequestParser(self.cfg, self.sock, self.client)

    def set_timeout(self):
        # set the timeout
        self.timeout = time.time() + self.cfg.keepalive

    def close(self):
        util.close(self.sock)


class PollableDeferredMethodCaller(object):
    def __init__(self):
        self.fds = []
        self.method_queue = None

    def init(self):
        self.fds = os.pipe2(os.O_NONBLOCK)
        self.method_queue = queue.SimpleQueue()

    def get_fd(self):
        return self.fds[0]

    def defer(self, callback):
        self.method_queue.put(callback)
        os.write(self.fds[1], b'0')

    def run_callbacks(self, max_callbacks=8):
        zeroes = os.read(self.fds[0], max_callbacks)
        for _ in range(0, len(zeroes)):
            method = self.method_queue.get()
            method()


class ThreadWorker(base.Worker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_connections = self.cfg.worker_connections
        self.max_keepalived = self.cfg.worker_connections - self.cfg.threads
        # initialise the pool
        self.thread_pool = None
        self.poller = None
        self.futures = deque()
        self.keepalived_conns = deque()
        self.nr_conns = 0
        self.main_thread_caller = PollableDeferredMethodCaller()

    @classmethod
    def check_config(cls, cfg, log):
        max_keepalived = cfg.worker_connections - cfg.threads

        if max_keepalived <= 0 and cfg.keepalive:
            log.warning("No keepalived connections can be handled. " +
                        "Check the number of worker connections and threads.")

    def init_process(self):
        self.thread_pool = self.get_thread_pool()
        self.poller = selectors.DefaultSelector()
        self.main_thread_caller.init()
        for sock in self.sockets:
            sock.setblocking(False)
        super().init_process()

    def get_thread_pool(self):
        """Override this method to customize how the thread pool is created"""
        return futures.ThreadPoolExecutor(max_workers=self.cfg.threads)

    def handle_quit(self, sig, frame):
        self.alive = False
        # worker_int callback
        self.cfg.worker_int(self)
        self.thread_pool.shutdown(False)
        time.sleep(0.1)
        sys.exit(0)

    def set_accept_enabled(self, enabled, is_initial_callback=False):
        if is_initial_callback:
            assert enabled
            for sock in self.sockets:
                self.poller.register(sock, selectors.EVENT_READ, self.accept)
        else:
            for sock in self.sockets:
                self.poller.modify(sock, selectors.EVENT_READ if enabled else 0, self.accept)

    def accept(self, listener):
        try:
            while self.nr_conns < self.worker_connections:
                sock, client = listener.accept()
                if not sock:
                    break

                # initialize the connection object
                conn = TConn(self.cfg, sock, client, listener.getsockname())
                self.nr_conns += 1

                # wait until socket is readable
                self.poller.register(conn.sock, selectors.EVENT_READ,
                                     partial(self.on_client_socket_readable, conn))

            if self.nr_conns >= self.worker_connections:
                self.set_accept_enabled(False)
        except EnvironmentError as e:
            if e.errno not in (errno.EAGAIN, errno.ECONNABORTED,
                               errno.EWOULDBLOCK):
                raise

    def on_client_socket_readable(self, conn, client):
        self.poller.unregister(client)

        if conn.initialized:
            self.keepalived_conns.remove(conn)
        conn.init()

        fs = self.thread_pool.submit(self.handle, conn)

        self.futures.append(fs)
        fs.add_done_callback(
            lambda fut: self.main_thread_caller.defer(partial(self.finish_request, conn, fut)))

    def murder_keepalived(self):
        now = time.time()
        while self.keepalived_conns:
            conn = self.keepalived_conns[0]

            delta = conn.timeout - now
            if delta > 0:
                break

            self.nr_conns -= 1
            self.keepalived_conns.popleft()
            self.poller.unregister(conn.sock)
            conn.close()

    def is_parent_alive(self):
        # If our parent changed then we shut down.
        if self.ppid != os.getppid():
            self.log.info("Parent changed, shutting down: %s", self)
            return False
        return True

    def run(self):
        self.set_accept_enabled(True, is_initial_callback=True)
        self.poller.register(self.main_thread_caller.get_fd(),
                             selectors.EVENT_READ,
                             self.main_thread_caller.run_callbacks)

        while self.alive:
            # notify the arbiter we are alive
            self.notify()

            for key, _ in self.poller.select(1.0):
                callback = key.data
                callback(key.fileobj)

            if not self.is_parent_alive():
                break

            # handle keepalive timeouts
            self.murder_keepalived()

            if self.nr_conns < self.worker_connections:
                self.set_accept_enabled(True)

        self.thread_pool.shutdown(False)
        self.poller.close()

        futures.wait(self.futures, timeout=self.cfg.graceful_timeout)

        for s in self.sockets:
            s.close()

    def finish_request(self, conn, fs):
        self.futures.remove(fs)

        if fs.cancelled():
            self.nr_conns -= 1
            conn.close()
            return

        try:
            keepalive = fs.result()
            # if the connection should be kept alived add it
            # to the eventloop and record it
            if keepalive and self.alive:
                # register the connection
                conn.set_timeout()

                self.keepalived_conns.append(conn)
                self.poller.register(conn.sock, selectors.EVENT_READ,
                                     partial(self.on_client_socket_readable, conn))
            else:
                self.nr_conns -= 1
                conn.close()
        except Exception:
            # an exception happened, make sure to close the
            # socket.
            self.nr_conns -= 1
            conn.close()

    def handle(self, conn):
        req = None
        try:
            req = next(conn.parser)
            if not req:
                return False

            # handle the request
            return self.handle_request(req, conn)
        except http.errors.NoMoreData as e:
            self.log.debug("Ignored premature client disconnection. %s", e)

        except StopIteration as e:
            self.log.debug("Closing connection. %s", e)
        except ssl.SSLError as e:
            if e.args[0] == ssl.SSL_ERROR_EOF:
                self.log.debug("ssl connection closed")
                conn.sock.close()
            else:
                self.log.debug("Error processing SSL request.")
                self.handle_error(req, conn.sock, conn.client, e)

        except EnvironmentError as e:
            if e.errno not in (errno.EPIPE, errno.ECONNRESET, errno.ENOTCONN):
                self.log.exception("Socket error processing request.")
            else:
                if e.errno == errno.ECONNRESET:
                    self.log.debug("Ignoring connection reset")
                elif e.errno == errno.ENOTCONN:
                    self.log.debug("Ignoring socket not connected")
                else:
                    self.log.debug("Ignoring connection epipe")
        except Exception as e:
            self.handle_error(req, conn.sock, conn.client, e)

        return False

    def handle_request(self, req, conn):
        environ = {}
        resp = None
        try:
            self.cfg.pre_request(self, req)
            request_start = datetime.now()
            resp, environ = wsgi.create(req, conn.sock, conn.client,
                                        conn.server, self.cfg)
            environ["wsgi.multithread"] = True
            self.nr += 1
            if self.nr >= self.max_requests:
                if self.alive:
                    self.log.info("Autorestarting worker after current request.")
                    self.alive = False
                resp.force_close()

            if not self.alive or not self.cfg.keepalive:
                resp.force_close()
            elif len(self.keepalived_conns) >= self.max_keepalived:
                resp.force_close()

            respiter = self.wsgi(environ, resp.start_response)
            try:
                if isinstance(respiter, environ['wsgi.file_wrapper']):
                    resp.write_file(respiter)
                else:
                    for item in respiter:
                        resp.write(item)

                resp.close()
            finally:
                request_time = datetime.now() - request_start
                self.log.access(resp, req, environ, request_time)
                if hasattr(respiter, "close"):
                    respiter.close()

            if resp.should_close():
                self.log.debug("Closing connection.")
                return False
        except EnvironmentError:
            # pass to next try-except level
            util.reraise(*sys.exc_info())
        except Exception:
            if resp and resp.headers_sent:
                # If the requests have already been sent, we should close the
                # connection to indicate the error.
                self.log.exception("Error handling request")
                try:
                    conn.sock.shutdown(socket.SHUT_RDWR)
                    conn.sock.close()
                except EnvironmentError:
                    pass
                raise StopIteration()
            raise
        finally:
            try:
                self.cfg.post_request(self, req, environ, resp)
            except Exception:
                self.log.exception("Exception in post_request hook")

        return True
