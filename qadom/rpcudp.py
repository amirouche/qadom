# Copyright (c) 2014 Brian Muller
# Copyright (c) 2019 Amirouche Boubekki

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
from uuid import uuid4

import msgpack


log = logging.getLogger(__name__)


class RPCUDPException(Exception):
    pass


class MalformedMessage(RPCUDPException):
    """Message does not contain what is expected.

    """


class NoReplyException(RPCUDPException):
    """The recipient did not reply.

    """


class RPCProtocol(asyncio.DatagramProtocol):
    def __init__(self, wait_timeout=5):
        """wait_timeout: Consider it a connetion failure if no response within
        this time window.

        """
        self._wait_timeout = wait_timeout
        self._outstanding = {}
        self._transport = None
        self.procedures = {}

    def register(self, proc):
        self.procedures[proc.__name__.encode("utf8")] = proc

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, datagram, address):
        log.debug("received datagram from %s", address)
        if len(datagram) > 8192:
            log.warning("received big datagram from %r", address)
            return

        try:
            uid, type, data = msgpack.unpackb(datagram, use_list=False)
        except msgpack.UnpackException:
            log.debug("received malformed packed from %r", address)
            return

        # process message as a new request or a response
        if type == b"\x00":
            asyncio.ensure_future(self._accept_request(uid, data, address))
        elif type == b"\x01":
            self._accept_response(uid, data, address)
        else:
            log.debug("Received unknown message from %s, ignoring", address)

    def _accept_response(self, uid, data, address):
        try:
            future, timeout = self._outstanding[uid]
        except KeyError:
            log.warning("received unknown message %r from %r; ignoring", data, address)
        else:
            log.debug(
                "received response %r for message uid %r from %r", data, uid, address
            )
            timeout.cancel()
            future.set_result(data)
            del self._outstanding[uid]

    async def _accept_request(self, uid, data, address):
        # TODO: more validation
        if not isinstance(data, tuple) or len(data) != 2:
            raise MalformedMessage("Could not read packet from {}".format(address))
        name, args = data
        try:
            procedure = self.procedures[name]
        except KeyError:
            log.warning("has no callable method %r; ignoring request", name)
        else:
            response = await procedure(address, *args)
            log.debug(
                "sending response %s for msg id %s to %s",
                response,
                b64encode(uid),
                address,
            )
            data = msgpack.packb([uid, b"\x01", response])
            self._transport.sendto(data, address)

    def _timeout(self, uid):
        args = (b64encode(uid), self._wait_timeout)
        log.error("Did not received reply for msg id %s within %i seconds", *args)
        future = self._outstanding[uid][0]
        future.set_exception(NoReplyException())
        del self._outstanding[uid]

    def rpc(self, address, name, *args):
        """Call `name` procedure on remote `address` with `args`.

        Return a future, that might raise a `NoReplyException` if
        there is no reply after `wait_timeout`.

        """
        uid = uuid4().bytes
        txdata = msgpack.packb([uid, b"\x00", [name, args]])
        if len(txdata) > 8192:
            msg = "Total length message cannot exceed 8K"
            raise MalformedMessage(msg)
        log.debug(
            "calling remote function %s on %s (uid %s)", name, address, b64encode(uid)
        )
        self._transport.sendto(txdata, address)

        loop = asyncio.get_event_loop()
        if hasattr(loop, "create_future"):
            future = loop.create_future()
        else:
            future = asyncio.Future()
        timeout = loop.call_later(self._wait_timeout, self._timeout, uid)
        self._outstanding[uid] = (future, timeout)
        return future
