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

from time import sleep
from logging import getLogger

from gofer.messaging import auth
from gofer.messaging.model import Envelope, is_valid, search
from gofer.transport.consumer import Ack
from gofer.transport.rabbitmq.endpoint import Endpoint, reliable


log = getLogger(__name__)


# --- constants --------------------------------------------------------------


DELAY = 0.0010
MAX_DELAY = 2.0
DELAY_MULTIPLIER = 1.2


# --- consumers --------------------------------------------------------------


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
        self.queue = queue

    @reliable
    def get(self):
        """
        Get the next message from the queue.
        :return: The next message or None.
        :rtype: librabbitmq.Message
        """
        channel = self.channel()
        self.queue.declare(self.url)
        message = channel.basic_get(self.queue.name)
        if message and not auth.is_valid(self.authenticator, message.body):
            message = None
            self.ack(message)
        return message

    @reliable
    def next(self, timeout=90):
        """
        Get the next envelope from the queue.
        :param timeout: The read timeout in seconds.
        :type timeout: int
        :return: A tuple of: (envelope, ack())
        :rtype: (Envelope, callable)
        """
        delay = DELAY
        timer = float(timeout or 0)
        while True:
            message = self.get()
            if message:
                envelope = Envelope()
                envelope.load(message.body)
                if is_valid(envelope):
                    log.debug('{%s} read next:\n%s', self.id(), envelope)
                    return envelope, Ack(self, message)
                else:
                    self.ack(message)
            if timer > 0:
                sleep(delay)
                timer -= delay
                if delay < MAX_DELAY:
                    delay *= DELAY_MULTIPLIER
            else:
                break
        return None, None

    def search(self, sn, timeout=90):
        """
        Search the reply queue for the envelope with the matching serial #.
        :param sn: The expected serial number.
        :type sn: str
        :param timeout: The read timeout.
        :type timeout: int
        :return: The next envelope.
        :rtype: Envelope
        """
        return search(self, sn, timeout)
