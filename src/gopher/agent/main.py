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

import sys
import os
from getopt import getopt
from gopher.client import *
from gopher.client.config import Config
from gopher.client.lock import Lock, LockFailed
from gopher.client.agent import *
from gopher.client.agent.action import Action
from gopher.client.agent.plugin import PluginLoader
from gopher.client.agent.plugins import *
from gopher.client.logutil import getLogger
from gopher.messaging import Queue
from gopher.messaging.broker import Broker
from gopher.messaging.base import Agent as Base
from gopher.messaging.consumer import RequestConsumer
from time import sleep
from threading import Thread

log = getLogger(__name__)


class ActionThread(Thread):
    """
    Run actions independantly of main thread.
    @ivar actions: A list of actions to run.
    @type actions: [L{Action},..]
    """
    
    def __init__(self, actions):
        """
        @param actions: A list of actions to run.
        @type actions: [L{Action},..]
        """
        self.actions = actions
        Thread.__init__(self, name='Actions')
   
    def run(self):
        """
        Run actions.
        """
        while True:
            for action in self.actions:
                action()
            sleep(10)
            

class Agent(Base):
    """
    Pulp agent.
    """

    def __init__(self, actions=[]):
        id = self.id()
        actionThread = ActionThread(actions)
        actionThread.start()
        cfg = Config()
        queue = Queue(id)
        url = cfg.messaging.url
        if url and isinstance(url, str):
            broker = Broker.get(url)
            broker.cacert = cfg.messaging.cacert
            broker.clientcert = cfg.messaging.clientcert
            consumer = RequestConsumer(queue, url=url)
            Base.__init__(self, consumer)
        else:
            log.warn('agent {%s} has messaging disabled.', id)
        log.info('agent {%s} - started.', id)
        actionThread.join()

    def id(self):
        """
        Get agent id.
        @return: The agent UUID.
        """
        cid = ConsumerId()
        while ( not cid.uuid ):
            log.info('Not registered.')
            sleep(60)
            cid.read()
        return cid.uuid


class AgentLock(Lock):
    """
    Agent lock ensure that agent only has single instance running.
    @cvar PATH: The lock file absolute path.
    @type PATH: str
    """

    PATH = '/var/run/gopherd.pid'

    def __init__(self):
        Lock.__init__(self, self.PATH)


def start(daemon=True):
    """
    Agent main.
    Add recurring, time-based actions here.
    All actions must be subclass of L{action.Action}.
    """
    lock = AgentLock()
    try:
        lock.acquire(wait=False)
    except LockFailed, e:
        raise Exception('Agent already running')
    if daemon:
        daemonize(lock)
    try:
        pl = PluginLoader()
        pl.load()
        actions = []
        for cls, interval in Action.actions:
            action = cls(**interval)
            actions.append(action)
        agent = Agent(actions)
        agent.close()
    finally:
        lock.release()

def usage():
    """
    Show usage.
    """
    s = []
    s.append('\ngopherd <optoins>')
    s.append('  -h, --help')
    s.append('      Show help')
    s.append('  -c, --console')
    s.append('      Run in the foreground and not as a daemon.')
    s.append('      default: 0')
    s.append('\n')
    print '\n'.join(s)

def daemonize(lock):
    """
    Daemon configuration.
    """
    pid = os.fork()
    if pid == 0: # child
        os.setsid()
        os.chdir('/')
        os.close(0)
        os.close(1)
        os.close(2)
        dn = os.open('/dev/null', os.O_RDWR)
        os.dup(dn)
        os.dup(dn)
        os.dup(dn)
    else: # parent
        lock.update(pid)
        os.waitpid(pid, os.WNOHANG)
        os._exit(0)

def main():
    daemon = True
    opts, args = getopt(sys.argv[1:], 'hc', ['help','console'])
    for opt,arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(0)
        if opt in ('-c', '--console'):
            daemon = False
            continue
    start(daemon)

if __name__ == '__main__':
    main()
