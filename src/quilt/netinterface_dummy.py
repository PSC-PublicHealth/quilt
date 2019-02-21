#! /usr/bin/env python

###################################################################################
# Copyright   2015, Pittsburgh Supercomputing Center (PSC).  All Rights Reserved. #
# =============================================================================== #
#                                                                                 #
# Permission to use, copy, and modify this software and its documentation without #
# fee for personal use within your organization is hereby granted, provided that  #
# the above copyright notice is preserved in all copies and that the copyright    #
# and this permission notice appear in supporting documentation.  All other       #
# restrictions and obligations are defined in the GNU Affero General Public       #
# License v3 (AGPL-3.0) located at http://www.gnu.org/licenses/agpl-3.0.html  A   #
# copy of the license is also provided in the top level of the source directory,  #
# in the file LICENSE.txt.                                                        #
#                                                                                 #
###################################################################################

import sys
import logging
import netinterface_base as nib

_logger = logging.getLogger(__name__)


class DummyCommException(Exception):
    pass


class DummyComm(object):
    @property
    def rank(self):
        return 0
    @property
    def size(self):
        return 1
    def Abort(self, code):
        sys.exit(code)
    def bcast(self, data, root=0):
        if root != 0:
            raise DummyCommException('There is no partner to share with')
        return data
    def allgather(self, data):
        return [data]
            
        
def getCommWorld():
    """Provide easy access to the world to packages that don't want to know about MPI"""
    return DummyComm()


class NetworkInterface(object):
    MPI_TAG_MORE = 1
    MPI_TAG_END = 2

    #maxChunksPerMsg = 32
    maxChunksPerMsg = 24
    irecvBufferSize = 1024 * 1024

    def __init__(self, comm, deterministic=False):
        assert isinstance(comm, DummyComm), 'Tried to build a dummy netinterface with a real communicator?'
        self.comm = comm
        self.vclock = nib.VectorClock(self.comm.size, self.comm.rank)
        self.outgoingList = []  # for messages to other addrs on this rank
        self.clientIncomingCallbacks = {}
        self.deterministic = deterministic
        self.incomingLclMessages = []
        self.doneSignalSent = False

    def getGblAddr(self, lclId):
        return nib.GblAddr(self.comm.rank, lclId)

    def isLocal(self, gblAddr):
        return True

    def barrier(self):
        pass

    def enqueue(self, msgType, thing, srcAddr, gblAddr):
        if not self.isLocal(gblAddr):
            raise DummyCommException('Cannot enqueue, this is a dummy network interface.')
        self.outgoingList.append((srcAddr, gblAddr, msgType, thing))

    def expect(self, srcAddr, destAddr, handleIncoming):
        """
        the handleIncoming is a callback with the signature

            handleIncoming(msgType, incomingTuple)

        There is no way to drop a rank from the expected source set because one can
        never be sure there is no straggler message from that rank
        """
        if not self.isLocal(srcAddr):
            raise DummyCommException('got expect request from a foreign rank but there are none')
        assert destAddr.rank == self.comm.rank, "Cannot deliver to foreign object %s" % destAddr
        self.clientIncomingCallbacks[(srcAddr.lclId, destAddr.lclId)] = handleIncoming

    def startRecv(self):
        pass

    def _innerRecv(self, tpl):
        msgType, srcTag, destTag, partTpl = tpl
        _logger.debug('msg type %s arrived from %s for %s' % (msgType, srcTag, destTag))
        self.clientIncomingCallbacks[(srcTag.lclId, destTag.lclId)](msgType, partTpl)

    def finishRecv(self):
        self.vclock.incr()  # must happen before incoming messages arrive
        _logger.debug('%d local messages' % len(self.incomingLclMessages))
        for tpl in self.incomingLclMessages:
            self._innerRecv(tpl)
        self.incomingLclMessages = []

    def startSend(self):
        msgList = self.outgoingList
        self.outgoingList = []
        if self.deterministic:
            msgList.sort()
        for srcTag, destTag, msgType, cargo in msgList:
            self.incomingLclMessages.append((msgType, srcTag, destTag, cargo))

    def finishSend(self):
        pass

    def sendDoneSignal(self):
        """
        This routine signals all partners of this NetworkInterface that it is 'done' and ready to
        close communication.  It can be called repeatedly; calls after the first have no effect.

        The return value is boolean, True if all partners have also sent the 'done' signal and
        False otherwise.  Thus one would typically call it in a communication loop, and exit the
        loop when it returns True.

        Once the 'done' signal has been sent, there is no way to return the NetworkInterface to the
        not-done state.
        """
        self.doneSignalSent = True
        return True
