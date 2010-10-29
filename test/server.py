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

from gopher.messaging import Queue
from gopher.messaging.base import Container
from gopher.messaging.producer import Producer
from gopher.messaging.window import *
from gopher.proxy import Agent
from time import sleep
from datetime import datetime as dt
from datetime import timedelta as delta
from logging import INFO, basicConfig, getLogger

basicConfig(filename='/tmp/gopher.log', level=INFO)

log = getLogger(__name__)



def demo(agent):
    print agent.admin.hello()
    
    dog = agent.Dog()
    repolib = agent.RepoLib()
    print agent.dog.bark('RUF')
    print dog.bark('hello')
    print dog.wag(3)
    print dog.bark('hello again')
    print repolib.update()
    try:
        print repolib.updated()
    except Exception, e:
        log.info('failed:', exc_info=True)
        print e
    try:
        print dog.notpermitted()
    except Exception, e:
        log.info('failed:', exc_info=True)
        print e

def later(**offset):
    return dt.utcnow()+delta(**offset)

if __name__ == '__main__':
    # synchronous
    print '(demo) synchronous'
    agent = Agent('123')
    demo(agent)
    agent.delete()
    agent = None

    # asynchronous (fire and forget)
    print '(demo) asynchronous fire-and-forget'
    agent = Agent('123', async=True)
    demo(agent)

    # asynchronous
    print '(demo) asynchronous'
    tag = 'xyz'
    window = Window(begin=dt.utcnow(), minutes=1)
    agent = Agent('123', ctag=tag, window=window)
    demo(agent)

    # asynchronous
    print '(demo) group asynchronous'
    tag = 'xyz'
    group = ('123', 'ABC',)
    window = Window(begin=dt.utcnow(), minutes=1)
    agent = Agent(group, ctag=tag)
    demo(agent)

    # future
    print 'maintenance window'
    dog = agent.Dog()

    # group 2
    print 'group 2'
    begin = later(seconds=20)
    window = Window(begin=begin, minutes=10)
    opts = dict(window=window, any='group 2')
    print dog.bark('hello', **opts)
    print dog.wag(3, **opts)
    print dog.bark('hello again', **opts)

    # group 1

    print 'group 1'
    begin = later(seconds=10)
    window = Window(begin=begin, minutes=10)
    opts = dict(window=window, any='group 1')
    print dog.bark('hello', **opts)
    print dog.wag(3, **opts)
    print dog.bark('hello again', **opts)

    agent = None
