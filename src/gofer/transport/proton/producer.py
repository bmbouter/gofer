# Copyright (c) 2013 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (GPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of GPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.

from logging import getLogger

from proton import Message

from gofer.messaging.model import getuuid, VERSION, Envelope
from gofer.transport.proton.endpoint import Endpoint


log = getLogger(__name__)


# --- utils ------------------------------------------------------------------


def milliseconds(seconds):
    if isinstance(seconds, int):
        return seconds * 1000
    else:
        return 0


def send(endpoint, destination, ttl=None, **body):
    """
    Send a message.
    :param endpoint: An AMQP endpoint.
    :type endpoint: Endpoint
    :param destination: An AMQP destination.
    :type destination: gofer.transport.model.Destination
    :param ttl: Time to Live (seconds)
    :type ttl: float
    :keyword body: envelope body.
    :return: The message serial number.
    :rtype: str
    """
    sn = getuuid()
    amqp_destination = destination.exchange
    routing_key = destination.routing_key
    routing = (endpoint.id(), destination.dict())
    envelope = Envelope(sn=sn, version=VERSION, routing=routing)
    envelope += body
    messenger = endpoint.messenger()
    message = Message()
    message.ttl = milliseconds(ttl)
    message.address = '/'.join((str(endpoint.url), amqp_destination))
    message.subject = routing_key
    message.body = envelope.dump()
    messenger.put(message)
    messenger.send()
    log.debug('{%s} sent (%s)\n%s', endpoint.id(), routing_key, envelope)
    return sn


# --- producers --------------------------------------------------------------


class Producer(Endpoint):
    """
    An AMQP (message producer.
    """

    def send(self, destination, ttl=None, **body):
        """
        Send a message.
        :param destination: An AMQP destination.
        :type destination: gofer.transport.model.Destination
        :param ttl: Time to Live (seconds)
        :type ttl: float
        :keyword body: envelope body.
        :return: The message serial number.
        :rtype: str
        """
        return send(self, destination, ttl, **body)

    def broadcast(self, destinations, ttl=None, **body):
        """
        Broadcast a message to (N) queues.
        :param destinations: A list of AMQP destinations.
        :type destinations: [gofer.transport.node.Destination,..]
        :param ttl: Time to Live (seconds)
        :type ttl: float
        :keyword body: envelope body.
        :return: A list of (addr, sn).
        :rtype: list
        """
        sns = []
        for dst in destinations:
            sn = send(self, dst, ttl, **body)
            sns.append((repr(dst), sn))
        return sns


class BinaryProducer(Endpoint):

    def send(self, destination, content, ttl=None):
        routing_key = destination.routing_key
        messenger = self.messenger()
        message = Message()
        message.ttl = milliseconds(ttl)
        message.address = '/'.join((str(self.url), routing_key))
        message.body = content
        messenger.put(message)
        messenger.send()
        log.debug('{%s} sent (binary)\n%s', self.id(), routing_key)

    def broadcast(self, destinations, content, ttl=None):
        for dst in destinations:
            self.send(dst, content, ttl)