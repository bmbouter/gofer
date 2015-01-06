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

from time import time
from threading import Thread, local as Local
from logging import getLogger

from gofer.rmi.window import *
from gofer.rmi.tracker import Tracker
from gofer.rmi.store import Pending
from gofer.rmi.dispatcher import Return, PluginNotFound
from gofer.rmi.threadpool import Direct
from gofer.messaging import Document, Producer
from gofer.metrics import Timer, timestamp


log = getLogger(__name__)


class Task:
    """
    An RMI task to be scheduled on the plugin thread pool.
    :ivar plugin: A plugin.
    :type plugin: gofer.agent.plugin.Plugin
    :ivar request: A gofer messaging request.
    :type request: Document
    :ivar commit: Transaction commit function.
    :type commit: callable
    :ivar window: The window in which the task is valid.
    :type window: dict
    :ivar ts: Timestamp
    :type ts: float
    """
    
    context = Local()

    @staticmethod
    def _producer(plugin):
        """
        Get a configured producer.
        :param plugin: A plugin.
        :type plugin: gofer.agent.plugin.Plugin
        :return: A producer.
        :rtype: Producer
        """
        producer = Producer(plugin.url)
        producer.authenticator = plugin.authenticator
        return producer

    def __init__(self, plugin, request, commit):
        """
        :param plugin: A plugin.
        :type plugin: gofer.agent.plugin.Plugin
        :param request: The inbound request to be dispatched.
        :type request: Document
        :param commit: Transaction commit function.
        :type commit: callable
        """
        self.plugin = plugin
        self.request = request
        self.commit = commit
        self.window = request.window
        self.producer = self._producer(plugin)
        self.ts = time()
        
    def __call__(self):
        """
        Dispatch received request.
        """
        request = self.request
        self.context.sn = request.sn
        self.context.progress = Progress(self)
        self.context.cancelled = Cancelled(request.sn)
        self.producer.open()
        try:
            self.__call()
        finally:
            self.context.sn = None
            self.context.progress = None
            self.context.cancelled = None
            self.producer.close()

    def __call(self):
        """
        Dispatch received request.
        """
        request = self.request
        try:
            self.window_missed()
            self.send_started(request)
            result = self.plugin.dispatch(request)
            self.commit(request.sn)
            self.send_reply(request, result)
        except WindowMissed:
            self.commit(request.sn)
            log.info('window missed: %s', request)
            self.send_reply(request, Return.exception())

    def window_missed(self):
        """
        Check the window.
        :raise WindowPending: when window in the future.
        :raise WindowMissed: when window missed.
        """
        w = self.window
        if not isinstance(w, dict):
            return
        window = Window(w)
        request = self.request
        if window.past():
            raise WindowMissed(request.sn)

    def send_started(self, request):
        """
        Send the a status update if requested.
        :param request: The received request.
        :type request: Document
        """
        sn = request.sn
        data = request.data
        route = request.replyto
        if not route:
            return
        try:
            self.producer.send(
                route,
                sn=sn,
                data=data,
                status='started',
                timestamp=timestamp())
        except Exception:
            log.exception('send (started), failed')
            
    def send_reply(self, request, result):
        """
        Send the reply if requested.
        :param request: The received request.
        :type request: Document
        :param result: The request result.
        :type result: object
        """
        sn = request.sn
        data = request.data
        ts = request.ts
        now = time()
        duration = Timer(ts, now)
        route = request.replyto
        log.info('sn=%s processed in: %s', sn, duration)
        if not route:
            return
        try:
            self.producer.send(
                route,
                sn=sn,
                data=data,
                result=result,
                timestamp=timestamp())
        except Exception:
            log.exception('send failed: %s', result)


class TrashPlugin:
    """
    An *empty* plugin.
    Used when the appropriate plugin cannot be found.
    """

    def __init__(self, inbound):
        self.url = inbound.url
        self.queue = inbound.queue
        self.authenticator = None
        self.pool = Direct()
    
    def dispatch(self, request):
        try:
            log.info('request sn=%s, trashed', request.sn)
            raise PluginNotFound(self.queue)
        except PluginNotFound:
            return Return.exception()


class TrashProducer(object):
    """
    The producer used when an appropriate one cannot be found.
    """

    def send(self, *args, **kwargs):
        """
        Send replies into the bit bucket.
        """
        pass

    def close(self):
        pass


class Scheduler(Thread):
    """
    The pending request scheduler.
    Processes the *pending* queue.
    :ivar plugins: A collection of loaded plugins.
    :type plugins: list
    """
    
    def __init__(self, plugins):
        """
        :param plugins: A collection of loaded plugins.
        :type plugins: list
        """
        Thread.__init__(self, name='scheduler')
        self.plugins = plugins
        self.pending = Pending()
        self.setDaemon(True)

    def run(self):
        while True:
            request = self.pending.get()
            try:
                plugin = self.find_plugin(request)
                task = Task(plugin, request, self.pending.commit)
                plugin.pool.run(task)
            except Exception:
                self.pending.commit(request.sn)
                log.exception(request.sn)
        
    def find_plugin(self, request):
        """
        Find the plugin that provides the class specified in
        the *request* embedded in the request.  Returns
        EmptyPlugin when not found.
        :param request: A gofer messaging request.
        :type request: Document
        :return: The appropriate plugin.
        :rtype: gofer.agent.plugin.Plugin
        """
        inbound = Options(request.inbound)
        for plugin in self.plugins:
            if plugin.queue == inbound.queue:
                return plugin
        log.info('plugin not found for "%s"', inbound.queue)
        return TrashPlugin(inbound)
    

class Context:
    """
    Remote method invocation context.
    Provides call context to method implementations.
    """
    
    @staticmethod
    def current():
        return Task.context


class Progress:
    """
    Provides support for progress reporting.
    :ivar task: The current task.
    :type task: Task
    :ivar total: The total work units.
    :type total: int
    :ivar completed: The completed work units.
    :type completed: int
    :ivar details: The reported details.
    :type details: object
    """
    
    def __init__(self, task):
        """
        :param task: The current task.
        :type task: Task
        """
        self.task = task
        self.total = 0
        self.completed = 0
        self.details = {}

    @property
    def producer(self):
        """
        Get a producer.
        :return: An AMQP producer.
        :rtype: Producer
        """
        return self.task.producer

    def report(self):
        """
        Send the progress report.
        """
        sn = self.task.request.sn
        data = self.task.request.data
        route = self.task.request.replyto
        if not route:
            return
        try:
            self.producer.send(
                route,
                sn=sn,
                data=data,
                status='progress',
                total=self.total,
                completed=self.completed,
                details=self.details)
        except Exception:
            log.exception('send (progress), failed')


class Cancelled:
    """
    A callable added to the Context and used
    by plugin methods to check for cancellation.
    :ivar tracker: The cancellation tracker.
    :type tracker: Tracker
    """

    def __init__(self, sn):
        """
        :param sn: Serial number.
        :type sn: str
        """
        self.sn = sn
        self.tracker = Tracker()

    def __call__(self):
        return self.tracker.cancelled(self.sn)

    def __del__(self):
        try:
            self.tracker.remove(self.sn)
        except KeyError:
            # already cleaned up
            pass
