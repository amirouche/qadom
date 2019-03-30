# Copyright (c) 2014 Brian Muller

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import asyncio
import logging
from base64 import b64encode
from hashlib import sha1
from uuid import uuid4

import msgpack


log = logging.getLogger(__name__)


class MalformedMessage(Exception):
    """
    Message does not contain what is expected.
    """


class RPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, wait_timeout=5):
        """
        @param waitTimeout: Consider it a connetion failure if no response
        within this time window.
        """
        self._wait_timeout = wait_timeout
        self._outstanding = {}
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, datagram, addr):
        log.debug("received datagram from %s", addr)

        try:
            uid, type, data = msgpack.unpackb(datagram)
        except msgpack.UnpackException:
            log.debug("received malformed packed from %r", addr)
            return

        # process message as a new request or a response
        if type == b'\x00':
            asyncio.ensure_future(self._accept_request(uid, data, address))
        elif type == b'\x01':
            self._accept_response(uid, data, address)
        else:
            log.debug("Received unknown message from %s, ignoring", address)

    def _accept_response(self, uid, data, address):
        try:
            f, timeout = self._outstanding[uid]
        except KeyError:
            log.warning("received unknown message %r from %r; ignoring", data, address)
        else:
            log.debug("received response %r for message uid %r from %r", data, uid, address)
            timeout.cancel()
            f.set_result((True, data))
            del self._outstanding[uid]

    async def _accept_request(self, uid, data, address):
        # TODO: more validation
        if not isinstance(data, list) or len(data) != 2:
            raise MalformedMessage("Could not read packet: %r" % data)
        funcname, args = data
        f = getattr(self, "rpc_%s" % funcname, None)
        if f is None or not callable(f):
            msgargs = (self.__class__.__name__, funcname)
            log.warning("%s has no callable method " "rpc_%s; ignoring request", *msgargs)
            return

        response = await f(address, *args)
        log.debug("sending response %s for msg id %s to %s", response, b64encode(uid), address)
        data = msgpack.packb([uid, b'\x01', response])
        self.transport.sendto(data, address)

    def _timeout(self, uid):
        args = (b64encode(uid), self._wait_timeout)
        log.error("Did not received reply for msg id %s within %i seconds", *args)
        self._outstanding[uid][0].set_result((False, None))
        del self._outstanding[uid]

    def rpc(self, name, *args):
        uid = uuid4().bytes
        txdata = msgpack.packb([uid, b'\x00', [name, args]])
        if len(txdata) > 8192:
            msg = "Total length message cannot exceed 8K"
            raise MalformedMessage(msg)
        log.debug("calling remote function %s on %s (uid %s)", name, address, b64encode(uid))
        self.transport.sendto(txdata, address)

        loop = asyncio.get_event_loop()
        if hasattr(loop, 'create_future'):
            future = loop.create_future()
        else:
            future = asyncio.Future()
        timeout = loop.call_later(self._wait_timeout, self._timeout, uid)
        self._outstanding[uid] = (future, timeout)
        return future
