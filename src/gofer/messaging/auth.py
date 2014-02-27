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
Message authentication plumbing.
"""

from logging import getLogger

from gofer.messaging.model import Envelope


log = getLogger(__name__)


SIGNATURE = 'signature'


class Authenticator(object):
    """
    Document the message authenticator API.
    """

    def sign(self, message):
        """
        Sign the specified message.
        :param message: An AMQP message body.
        :type message: str
        :return: The message signature.
        :rtype: str
        """
        raise NotImplementedError()

    def is_valid(self, uuid, message, signature):
        """
        Validate the specified message and signature.
        :param uuid: The uuid of the sender.
        :type uuid: str
        :param message: An AMQP message body.
        :type message: str
        :param signature: A message signature.
        :type signature: str
        :return: True if validated.
        :rtype: bool
        """
        raise NotImplementedError()


def sign(authenticator, message):
    """
    Sign the envelope using the specified validator.
    Adds the SIGNATURE property.
    :param authenticator: A message authenticator.
    :type authenticator: Authenticator
    :param message: A (signed) json encoded AMQP message.
    :rtype message: str
    """
    if not authenticator:
        return message
    try:
        unsigned = Envelope()
        unsigned.load(message)
        unsigned.__dict__[SIGNATURE] = authenticator.sign(message)
        message = unsigned.dump()
    except Exception:
        log.debug(message, exc_info=True)
    return message


def is_valid(authenticator, message):
    """
    Validate the envelope using the specified validator.
    Extracts the SIGNATURE attribute
    :param authenticator: A message authenticator.
    :type authenticator: Authenticator
    :param message: A json encoded AMQP message.
    :rtype message: str
    :return: True if valid.
    :rtype: bool
    """
    if not authenticator:
        return True
    try:
        signed = Envelope()
        signed.load(message)
        uuid = signed.routing[0]
        signature = signed.__dict__.pop(SIGNATURE, '')
        original = signed.dump()
        valid = authenticator.is_valid(uuid, original, signature)
        if not valid:
            log.info(
                'message: sn=%s signature=%s, rejected',
                signed.sn,
                signed.signature)
        return valid
    except Exception:
        log.debug(message, exc_info=True)
        return False