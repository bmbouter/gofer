#! /usr/bin/env python
#
# Copyright (c) 2010 Red Hat, Inc.
#
# Authors: Jeff Ortel <jortel@redhat.com>
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
sys.path.append('../../')

from time import sleep
from gopher.messaging import Queue
from gopher.messaging.base import Agent as Base
from gopher.messaging.decorators import *
from gopher.messaging.consumer import RequestConsumer
from gopher.messaging.broker import Broker
from logging import INFO, basicConfig

basicConfig(filename='/tmp/gopher.log', level=INFO)

@remote
@alias(name='admin')
class AgentAdmin:

    @remotemethod
    def hello(self):
        s = []
        s.append('Hello, I am gopher agent.')
        s.append('Status: ready')
        return '\n'.join(s)

@remote
@alias(name=['repolib'])
class RepoLib:
    @remotemethod
    def update(self):
        print 'Repo updated'

@remote
@alias('dog')
class Dog:
    @remotemethod
    def bark(self, words):
        print 'Ruf %s' % words
        return 'Yes master.  I will bark because that is what dogs do. "%s"' % words

    @remotemethod
    def wag(self, n):
        for i in range(0, n):
            print 'wag'
        return 'Yes master.  I will wag my tail because that is what dogs do.'

    def notpermitted(self):
        print 'not permitted.'


class Agent(Base):
    def __init__(self, id):
        queue = Queue(id)
        #url = 'ssl://localhost:5674'
        url = 'tcp://localhost:5672'
        broker = Broker.get(url)
        broker.cacert = '/etc/pki/qpid/ca/ca.crt'
        broker.clientcert = '/etc/pki/qpid/client/client.pem'
        Base.__init__(self, RequestConsumer(queue, url=url))
        while True:
            sleep(10)
            print 'Agent: sleeping...'

if __name__ == '__main__':
    if len(sys.argv) > 1:
        cid = sys.argv[1]
    else:
        cid = '123'
    print 'starting agent (%s)' % cid
    agent = Agent(cid)
