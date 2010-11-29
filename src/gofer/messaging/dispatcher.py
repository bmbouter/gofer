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
"""
Provides RMI dispatcher classes.
"""

import sys
import inspect
import traceback as tb
from gofer.messaging import *
from gofer.messaging.decorators import Remote
from logging import getLogger


log = getLogger(__name__)


class ClassNotFound(Exception):
    """
    Target class not found.
    """

    def __init__(self, classname):
        Exception.__init__(self, classname)


class MethodNotFound(Exception):
    """
    Target method not found.
    """

    def __init__(self, classname, method):
        message = '%s.%s(), not found' % (classname, method)
        Exception.__init__(self, message)


class NotPermitted(Exception):
    """
    Permission denied or not visible.
    """

    def __init__(self, classname, method):
        message = '%s.%s(), not permitted' % (classname, method)
        Exception.__init__(self, message)


class Return(Envelope):
    """
    Return envelope.
    """

    @classmethod
    def succeed(cls, x):
        """
        Return successful
        @param x: The returned value.
        @type x: any
        @return: A return envelope.
        @rtype: L{Return}
        """
        return Return(retval=x)

    @classmethod
    def exception(cls):
        """
        Return raised exception.
        @return: A return envelope.
        @rtype: L{Return}
        """
        info = sys.exc_info()
        ex = '\n'.join(tb.format_exception(*info))
        return Return(exval=ex)

    def succeeded(self):
        """
        Test whether the return indicates success.
        @return: True when indicates success.
        @rtype: bool
        """
        return ( 'retval' in self )

    def failed(self):
        """
        Test whether the return indicates failure.
        @return: True when indicates failure.
        @rtype: bool
        """
        return ( not self.succeeded() )


class Request(Envelope):
    """
    An RMI request envelope.
    """
    pass


class RMI(object):
    """
    The RMI object performs the invocation.
    @ivar request: The request envelope.
    @type request: L{Request}
    @ivar catalog: A dict of class mappings.
    @type catalog: dict
    """

    def __init__(self, request, catalog):
        """
        @param request: The request envelope.
        @type request: L{Request}
        @param catalog: A dict of class mappings.
        @type catalog: dict
        """
        self.request = request
        self.catalog = catalog

    def resolve(self):
        """
        Resolve the class/method in the request.
        @return: A tuple (inst, method)
        @rtype: tuple
        """
        inst = self.getclass()
        method = self.getmethod(inst)
        return (inst, method)

    def getclass(self):
        """
        Get an instance of the class or module specified in
        the request using the catalog.
        @return: An instance.
        @rtype: (class|module)
        """
        key = self.request.classname
        inst = self.catalog.get(key, None)
        if inst is None:
            raise ClassNotFound(key)
        if inspect.isclass(inst):
            return inst()
        else:
            return inst

    def getmethod(self, inst):
        """
        Get method of the class specified in the request.
        Ensures that remote invocation is permitted.
        @param inst: A class or module object.
        @type inst: (class|module)
        @return: The requested method.
        @rtype: (method|function)
        """
        cn, fn = \
            (self.request.classname,
             self.request.method)
        if hasattr(inst, fn):
            method = getattr(inst, fn)
            if not self.permitted(method):
                raise NotPermitted(cn, fn)
            return method
        else:
            raise MethodNotFound(cn, fn)
        
    def permitted(self, method):
        """
        Get whether remote invocation of the specified method is permitted.
        @param method: The method in question.
        @type method: (method|function)
        @return: True if permitted.
        @rtype: bool
        """
        if inspect.ismethod(method):
            fn = method.im_func
        else:
            fn = method
        return ( hasattr(fn, 'remotepermitted') ) 

    def __call__(self):
        """
        Invoke the method.
        @return: The invocation result.
        @rtype: L{Return}
        """
        args, keywords = \
            (self.request.args,
             self.request.kws)
        try:
            inst, method = self.resolve()
            retval = method(*args, **keywords)
            return Return.succeed(retval)
        except:
            return Return.exception()

    def __str__(self):
        return str(self.request)

    def __repr__(self):
        return str(self)


class Dispatcher:
    """
    The remote invocation dispatcher.
    @ivar classes: The (catalog) of target classes.
    @type classes: list
    """

    def __init__(self):
        """
        """
        self.classes = {}

    def dispatch(self, request):
        """
        Dispatch the requested RMI.
        @param request: A request.
        @type request: L{Request}
        @return: The result.
        @rtype: any
        """
        request = Request(request)
        rmi = RMI(request, self.classes)
        log.info('dispatching:%s', rmi)
        return rmi()

    def register(self, *classes):
        """
        Register classes exposed as RMI targets.
        @param classes: A list of classes
        @type classes: [cls,..]
        @return self
        @rtype: L{Dispatcher}
        """
        for cls in classes:
            self.classes[cls.__name__] = cls
        return self
