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


"""
Provides async AMQP message consumer classes.
"""

from gofer.messaging import *
from gofer.messaging.dispatcher import Return, RemoteException
from gofer.messaging.consumer import Consumer
from logging import getLogger

log = getLogger(__name__)


class ReplyConsumer(Consumer):
    """
    A request, reply consumer.
    @ivar listener: An reply listener.
    @type listener: any
    """

    def start(self, listener):
        """
        Start processing messages on the queue and
        forward to the listener.
        @param listener: A reply listener.
        @type listener: L{Listener}
        """
        self.listener = listener
        Consumer.start(self)

    def dispatch(self, envelope):
        """
        Dispatch received request.
        @param envelope: The received envelope.
        @type envelope: L{Envelope}
        """
        try:
            reply = self.__getreply(envelope)
            reply.notify(self.listener)
        except Exception, e:
            log.exception(e)

    def __getreply(self, envelope):
        if envelope.status:
            return Status(envelope)
        result = Return(envelope.result)
        if result.succeeded():
            return Succeeded(envelope)
        else:
            return Failed(envelope)



class AsyncReply:
    """
    Asynchronous request reply.
    @ivar sn: The request serial number.
    @type sn: str
    @ivar origin: Which endpoint sent the reply.
    @type origin: str
    @ivar any: User defined (round-tripped) data.
    @type any: object
    """

    def __init__(self, envelope):
        """
        @param envelope: The received envelope.
        @type envelope: L{Envelope}
        """
        self.sn = envelope.sn
        self.origin = envelope.origin
        self.any = envelope.any

    def notify(self, listener):
        """
        Notify the specified listener.
        @param listener: The listener to notify.
        @type listener: L{Listener} or callable.
        """
        pass

    def __str__(self):
        s = []
        s.append(self.__class__.__name__)
        s.append('  sn : %s' % self.sn)
        s.append('  origin : %s' % self.origin)
        s.append('  user data : %s' % self.any)
        return '\n'.join(s)


class FinalReply(AsyncReply):
    """
    A (final) reply.
    """

    def notify(self, listener):
        if callable(listener):
            listener(self)
            return
        if self.succeeded():
            listener.succeeded(self)
        else:
            listener.failed(self)

    def succeeded(self):
        """
        Get whether the reply indicates success.
        @return: True when succeeded.
        @rtype: bool
        """
        return False

    def failed(self):
        """
        Get whether the reply indicates failure.
        @return: True when failed.
        @rtype: bool
        """
        return ( not self.succeeded() )

    def throw(self):
        """
        Throw contained exception.
        @raise Exception: When contained.
        """
        pass


class Succeeded(FinalReply):
    """
    Successful reply to asynchronous operation.
    @ivar retval: The returned value.
    @type retval: object
    """

    def __init__(self, envelope):
        """
        @param envelope: The received envelope.
        @type envelope: L{Envelope}
        """
        AsyncReply.__init__(self, envelope)
        reply = Return(envelope.result)
        self.retval = reply.retval

    def succeeded(self):
        return True

    def __str__(self):
        s = []
        s.append(AsyncReply.__str__(self))
        s.append('  retval:')
        s.append(str(self.retval))
        return '\n'.join(s)


class Failed(FinalReply):
    """
    Failed reply to asynchronous operation.  This reply
    indicates an exception was raised.
    @ivar exval: The returned exception.
    @type exval: object
    @see: L{Failed.throw}
    """

    def __init__(self, envelope):
        """
        @param envelope: The received envelope.
        @type envelope: L{Envelope}
        """
        AsyncReply.__init__(self, envelope)
        reply = Return(envelope.result)
        self.exval = RemoteException.instance(reply)

    def throw(self):
        raise self.exval

    def __str__(self):
        s = []
        s.append(AsyncReply.__str__(self))
        s.append('  exception:')
        s.append(str(self.exval))
        return '\n'.join(s)


class Status(AsyncReply):
    """
    Status changed for an asynchronous operation.
    @ivar status: The status.
    @type status: str
    @see: L{Failed.throw}
    """

    def __init__(self, envelope):
        """
        @param envelope: The received envelope.
        @type envelope: L{Envelope}
        """
        AsyncReply.__init__(self, envelope)
        self.status = 'started'

    def notify(self, listener):
        if callable(listener):
            listener(self)
        else:
            listener.status(self)

    def __str__(self):
        s = []
        s.append(AsyncReply.__str__(self))
        s.append('  status: %s' % str(self.status))
        return '\n'.join(s)


class Listener:
    """
    An asynchronous operation callback listener.
    """

    def succeeded(self, reply):
        """
        Async request succeeded.
        @param reply: The reply data.
        @type reply: L{Succeeded}.
        """
        pass

    def failed(self, reply):
        """
        Async request failed (raised an exception).
        @param reply: The reply data.
        @type reply: L{Failed}.
        """
        pass

    def status(self, reply):
        """
        Async request has started.
        @param reply: The request.
        @type reply: L{Status}.
        """
        pass
