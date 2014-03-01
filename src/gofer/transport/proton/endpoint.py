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

from threading import RLock
from logging import getLogger

from proton import Messenger

from gofer import synchronized
from gofer.transport.endpoint import Endpoint as Base


log = getLogger(__name__)


class Endpoint(Base):
    """
    Base class for an AMQP endpoint.
    :ivar uuid: The unique endpoint id.
    :type uuid: str
    :ivar url: The broker URL.
    :type url: str
    :ivar __mutex: The endpoint mutex.
    :type __mutex: RLock
    :ivar __messenger: An AMQP channel.
    :type __messenger: proton.Messenger
    """

    LOCALHOST = 'amqp://0.0.0.0'

    def __init__(self, uuid=None, url=LOCALHOST):
        """
        :param uuid: The endpoint uuid.
        :type uuid: str
        :param url: The broker url <transport>://<user>/<pass>@<host>:<port>.
        :type url: str
        """
        Base.__init__(self, uuid, url)
        self.__mutex = RLock()
        self.__messenger = None

    def id(self):
        """
        Get the endpoint id
        :return: The id.
        :rtype: str
        """
        return self.uuid

    @synchronized
    def messenger(self):
        """
        Get a channel for the open connection.
        :return: An open channel.
        :rtype: proton.Messenger
        """
        if self.__messenger is None:
            messenger = Messenger()
            messenger.incoming_window = 100
            messenger.start()
            self.__messenger = messenger
        return self.__messenger

    def open(self):
        """
        Open and configure the endpoint.
        """
        pass

    def ack(self, tracker):
        """
        Ack the specified message.
        :param tracker: An AMQP 1.0 message tracker.
        :type tracker: str
        """
        messenger = self.messenger()
        messenger.accept(tracker)

    @synchronized
    def close(self):
        """
        Close the endpoint.
        """
        try:
            messenger = self.__messenger
            self.__messenger = None
            if messenger is not None:
                messenger.stop()
        except Exception:
            # ignored
            pass

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()