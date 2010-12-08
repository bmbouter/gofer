#
# Copyright (c) 2010 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public License,
# version 2 (GPLv2). There is NO WARRANTY for this software, express or
# implied, including the implied warranties of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
# along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#
# Red Hat trademarks are not licensed under GPLv2. No permission is
# granted to use or replicate Red Hat trademarks that are incorporated
# in this software or its documentation.
#

"""
Contains request delivery policies.
"""

from gofer.messaging import *
from gofer.messaging.dispatcher import *
from gofer.messaging.consumer import Reader
from logging import getLogger

log = getLogger(__name__)



class RequestTimeout(Exception):
    """
    Request timeout.
    """

    def __init__(self, sn):
        """
        @param sn: The request serial number.
        @type sn: str
        """
        Exception.__init__(self, sn)
        

class RequestMethod:
    """
    Base class for request methods.
    @ivar producer: A queue producer.
    @type producer: L{gofer.messaging.producer.Producer}
    """

    def __init__(self, producer):
        """
        @param producer: A queue producer.
        @type producer: L{gofer.messaging.producer.Producer}
        """
        self.producer = producer

    def send(self, address, request, **any):
        """
        Send the request..
        @param address: The destination queue address.
        @type address: str
        @param request: A request to send.
        @type request: object
        @keyword any: Any (extra) data.
        """
        pass

    def broadcast(self, addresses, request, **any):
        """
        Broadcast the request.
        @param addresses: A list of destination queue addresses.
        @type addresses: [str,..]
        @param request: A request to send.
        @type request: object
        @keyword any: Any (extra) data.
        """
        pass

    def close(self):
        """
        Close and release all resources.
        """
        self.producer.close()


class Synchronous(RequestMethod):
    """
    The synchronous request method.
    This method blocks until a reply is received.
    @ivar reader: A queue reader used to read the reply.
    @type reader: L{gofer.messaging.consumer.Reader}
    """

    def __init__(self, producer, timeout):
        """
        @param producer: A queue producer.
        @type producer: L{gofer.messaging.producer.Producer}
        @param timeout: The request timeout (seconds).
        @type timeout: tuple
        """
        if not isinstance(timeout, (list,tuple)):
            timeout = (timeout, timeout)
        self.timeout = timeout
        self.queue = Queue(getuuid(), durable=False)
        RequestMethod.__init__(self, producer)
        reader = Reader(self.queue, url=self.producer.url)
        reader.start()
        self.reader = reader

    def send(self, destination, request, **any):
        """
        Send the request then read the reply.
        @param destination: The destination queue address.
        @type destination: str
        @param request: A request to send.
        @type request: object
        @keyword any: Any (extra) data.
        @return: The result of the request.
        @rtype: object
        @raise Exception: returned by the peer.
        """
        sn = self.producer.send(
            destination,
            replyto=str(self.queue),
            request=request,
            **any)
        self.__getstarted(sn)
        return self.__getreply(sn)

    def __getstarted(self, sn):
        envelope = self.reader.search(sn, self.timeout[0])
        if envelope:
            log.info('request (%s), started', sn)
        else:
            raise RequestTimeout(sn)

    def __getreply(self, sn):
        """
        Get the reply matched by serial number.
        @param sn: The request serial number.
        @type sn: str
        @return: The matched reply envelope.
        @rtype: L{Envelope}
        """
        envelope = self.reader.search(sn, self.timeout[1])
        if not envelope:
            raise RequestTimeout(sn)
        reply = Return(envelope.result)
        self.reader.ack()
        if reply.succeeded():
            return reply.retval
        else:
            raise RemoteException.instance(reply)


class Asynchronous(RequestMethod):
    """
    The asynchronous request method.
    """

    def __init__(self, producer, tag=None):
        """
        @param producer: A queue producer.
        @type producer: L{gofer.messaging.producer.Producer}
        @param tag: A reply I{correlation} tag.
        @type tag: str
        """
        RequestMethod.__init__(self, producer)
        self.tag = tag

    def send(self, destination, request, **any):
        """
        Send the specified request and redirect the reply to the
        queue for the specified reply I{correlation} tag.
        @param destination: The AMQP destination.
        @type destination: L{Destination}
        @param request: A request to send.
        @type request: object
        @keyword any: Any (extra) data.
        @return: The request serial number.
        @rtype: str
        """
        sn = self.producer.send(
                destination,
                replyto=self.__replyto(),
                request=request,
                **any)
        return sn

    def broadcast(self, destinations, request, **any):
        """
        Send the specified request and redirect the reply to the
        queue for the specified reply I{correlation} tag.
        @param destinations: A list of destinations.
        @type destinations: [L{Destination},..]
        @param request: A request to send.
        @type request: object
        @keyword any: Any (extra) data.
        """
        sns = self.producer.broadcast(
                destinations,
                replyto=self.__replyto(),
                request=request,
                **any)
        return sns

    def __replyto(self):
        """
        Get replyto based on the correlation I{tag}.
        @return: The replyto AMQP address.
        @rtype: str
        """
        if self.tag:
            queue = Queue(self.tag)
            return str(queue)
        else:
            return None
