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
from threading import RLock
from logging import getLogger

from amqplib.client_0_8 import AMQPChannelException

from gofer import synchronized
from gofer.transport.endpoint import Endpoint as Base
from gofer.transport.amqplib.broker import Broker, CONNECTION_EXCEPTIONS


log = getLogger(__name__)


# --- constants --------------------------------------------------------------


DELIVERY_TAG = 'delivery_tag'


# --- reconnect decorator ----------------------------------------------------


def reliable(fn):
    def _fn(endpoint, *args, **kwargs):
        while True:
            try:
                return fn(endpoint, *args, **kwargs)
            except CONNECTION_EXCEPTIONS, e:
                broker = Broker(endpoint.url)
                endpoint.close()
                broker.close()
                sleep(3)
                endpoint.channel()
    return _fn


# --- endpoint wrapper decorator ---------------------------------------------


def endpoint(fn):
    def _fn(url):
        _endpoint = Endpoint(url=url)
        try:
            return fn(_endpoint)
        finally:
            _endpoint.close()
    return _fn


# --- endpoint ---------------------------------------------------------------


class Endpoint(Base):
    """
    Base class for an AMQP endpoint.
    :ivar __mutex: The endpoint mutex.
    :type __mutex: RLock
    :ivar __channel: An AMQP channel.
    :type __channel: amqplib.Channel
    """

    LOCALHOST = 'amqp://localhost:5672'

    def __init__(self, uuid=None, url=None):
        """
        :param uuid: The endpoint uuid.
        :type uuid: str
        :param url: The broker url <transport>://<user>/<pass>@<host>:<port>.
        :type url: str
        """
        Base.__init__(self, uuid, url or Endpoint.LOCALHOST)
        self.__mutex = RLock()
        self.__channel = None

    def id(self):
        """
        Get the endpoint id
        :return: The id.
        :rtype: str
        """
        return self.uuid

    @synchronized
    def channel(self):
        """
        Get a channel for the open connection.
        :return: An open channel.
        :rtype: amqplib.Channel
        """
        if self.__channel is None:
            broker = Broker(self.url)
            conn = broker.connect()
            self.__channel = conn.channel()
        return self.__channel

    def open(self):
        """
        Open and configure the endpoint.
        """
        pass

    @reliable
    def ack(self, message):
        """
        Ack the specified message.
        :param message: An AMQP message.
        :type message: Message
        """
        channel = self.channel()
        channel.basic_ack(message.delivery_info[DELIVERY_TAG])

    @synchronized
    def close(self):
        """
        Close the endpoint.
        """
        try:
            channel = self.__channel
            self.__channel = None
            if channel is not None:
                channel.close()
        except (CONNECTION_EXCEPTIONS, AMQPChannelException):
            # ignored
            pass

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()