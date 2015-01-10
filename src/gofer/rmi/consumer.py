#
# Copyright (c) 2011 Red Hat, Inc.
#
# This software is licensed to you under the GNU Lesser General Public
# License as published by the Free Software Foundation; either version
# 2 of the License (LGPLv2) or (at your option) any later version.
# There is NO WARRANTY for this software, express or implied,
# including the implied warranties of MERCHANTABILITY,
# NON-INFRINGEMENT, or FITNESS FOR A PARTICULAR PURPOSE. You should
# have received a copy of LGPLv2 along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/lgpl-2.0.txt.
#
# Jeff Ortel <jortel@redhat.com>
#

from logging import getLogger

from gofer import Options
from gofer.rmi.store import Pending
from gofer.messaging import Consumer, Producer, Document
from gofer.metrics import timestamp

log = getLogger(__name__)


class RequestConsumer(Consumer):
    """
    Request consumer.
    Reads messages from AMQP, sends the accepted status then writes
    to local pending queue to be consumed by the scheduler.
    """

    def _rejected(self, code, description, document, details):
        """
        Called to process the received (invalid) document.
        This method intended to be overridden by subclasses.
        :param code: The rejection code.
        :type code: str
        :param description: rejection description
        :type description: str
        :param document: The received (json) document.
        :type document: str
        :param details: The explanation.
        :type details: str
        """
        details = dict(
            code=code,
            description=description,
            details=details
        )
        request = Document()
        request.load(document)
        self._send(request, 'rejected', **details)

    def _send(self, request, status, **details):
        """
        Send a status update.
        :param request: The received (json) request.
        :type request: Document
        :param status: The status to send ('accepted'|'rejected')
        :type status: str
        """
        route = request.replyto
        if not route:
            return
        try:
            producer = Producer(self.url)
            producer.authenticator = self.reader.authenticator
            producer.link(self.reader)
            producer.send(
                route,
                sn=request.sn,
                data=request.data,
                status=status,
                timestamp=timestamp(),
                **details)
        except Exception:
            log.exception('send (%s), failed', status)

    def dispatch(self, request):
        """
        Dispatch received request.
        Update the request: inject the inbound_url.
        :param request: The received request.
        :type request: Document
        """
        self._send(request, 'accepted')
        inbound = Options()
        inbound.url = self.url
        inbound.queue = self.queue.name
        request.inbound = inbound
        pending = Pending()
        pending.put(request)
