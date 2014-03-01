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

from gofer.messaging import auth
from gofer.messaging import model
from gofer.messaging.model import Envelope, search
from gofer.transport.proton.endpoint import Endpoint


log = getLogger(__name__)


class Reader(Endpoint):
    """
    An AMQP message reader.
    :ivar queue: The AMQP queue to read.
    :type queue: gofer.transport.model.Queue
    """

    def __init__(self, queue, **options):
        """
        :param queue: The queue to consumer.
        :type queue: gofer.transport.model.Queue
        :param options: Options passed to Endpoint.
        :type options: dict
        """
        Endpoint.__init__(self, **options)
        address = '/'.join((str(self.url), queue.name))
        messenger = self.messenger()
        messenger.subscribe(address)

    def get(self, timeout):
        """
        Get the next message from the queue.
        :return: The next message or None.
        :rtype: proton.Message
        """
        messenger = self.messenger()
        messenger.timeout = timeout
        messenger.recv(1)
        if messenger.incoming:
            message = Message()
            tracker = messenger.get(message)
            if message:
                try:
                    auth.validate(self.authenticator, message.body)
                except auth.ValidationFailed:
                    self.ack(tracker)
                    raise
            return tracker, message
        else:
            return None, None

    def next(self, timeout=90):
        """
        Get the next request from the queue.
        :param timeout: The read timeout in seconds.
        :type timeout: int
        :return: A tuple of: (request, ack())
        :rtype: (Envelope, callable)
        """
        tracker, message = self.get(timeout)
        if tracker:
            request = Envelope()
            request.load(message.body)
            try:
                model.validate(request)
            except model.InvalidRequest:
                self.ack(tracker)
                raise
            log.debug('{%s} read next:\n%s', self.id(), request)
            return request, Ack(self, tracker)
        return None, None

    def search(self, sn, timeout=90):
        """
        Search the reply queue for the request with the matching serial #.
        :param sn: The expected serial number.
        :type sn: str
        :param timeout: The read timeout.
        :type timeout: int
        :return: The next request.
        :rtype: Envelope
        """
        return search(self, sn, timeout)


class Ack:

    def __init__(self, endpoint, tracker):
        self.endpoint = endpoint
        self.tracker = tracker

    def __call__(self):
        self.endpoint.ack(self.tracker)

