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

from gofer.transport.model import Exchange as BaseExchange
from gofer.transport.model import Queue as BaseQueue


log = getLogger(__name__)


class Exchange(BaseExchange):

    @staticmethod
    def default():
        return Exchange('')

    @staticmethod
    def direct():
        return Exchange('')

    @staticmethod
    def topic():
        return Exchange('')

    @staticmethod
    def fanout():
        return Exchange('')


class Queue(BaseQueue):

    def __init__(self, name, exchange=None, routing_key=None):
        BaseQueue.__init__(
            self,
            name,
            exchange=exchange or Exchange.default(),
            routing_key=routing_key or name)
