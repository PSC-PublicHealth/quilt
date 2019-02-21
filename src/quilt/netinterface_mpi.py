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

_rhea_svn_id_ = "$Id$"

from mpi4py import MPI
import logging

logger = logging.getLogger(__name__)


def getCommWorld():
    """Provide easy access to the world to packages that don't want to know about MPI"""
    return MPI.COMM_WORLD




class NetworkInterface(object):
    MPI_TAG_MORE = 1
    MPI_TAG_END = 2

    #maxChunksPerMsg = 32
    maxChunksPerMsg = 24
    irecvBufferSize = 1024 * 1024

    def __init__(self, comm, deterministic=False):
        self.comm = comm
        self.vclock = VectorClock(self.comm.size, self.comm.rank)
        self.outgoingDict = {}
        self.outstandingSendReqs = []
        self.outstandingRecvReqs = []
        self.expectFrom = set()  # Other ranks sending to us directly
        self.clientIncomingCallbacks = {}
        self.deterministic = deterministic
        self.doneMsg = [(False, 0)]
        self.incomingLclMessages = []
        self.doneSignalSent = False
        self.doneSignalsSeen = 0
        self.doneMaxCycle = 0

    def getGblAddr(self, lclId):
        return GblAddr(self.comm.rank, lclId)

    def isLocal(self, gblAddr):
        return gblAddr.rank == self.comm.rank

    def barrier(self):
        self.comm.Barrier()

    def enqueue(self, msgType, thing, srcAddr, gblAddr):
        toRank = gblAddr.rank
        if toRank not in self.outgoingDict:
            self.outgoingDict[toRank] = []
        self.outgoingDict[toRank].append((srcAddr, gblAddr, msgType, thing))

    def expect(self, srcAddr, destAddr, handleIncoming):
        """
        the handleIncoming is a callback with the signature

            handleIncoming(msgType, incomingTuple)

        There is no way to drop a rank from the expected source set because one can
        never be sure there is no straggler message from that rank
        """
        if srcAddr.rank != self.comm.rank:
            self.expectFrom.add(srcAddr.rank)
        assert destAddr.rank == self.comm.rank, "Cannot deliver to foreign object %s" % destAddr
        self.clientIncomingCallbacks[(srcAddr.rank, srcAddr.lclId,
                                      destAddr.lclId)] = handleIncoming

    def startRecv(self):
        if self.deterministic:
            l = [a for a in self.expectFrom]
            l.sort()
            for srcRank in l:
                buf = bytearray(NetworkInterface.irecvBufferSize)
                self.outstandingRecvReqs.append(self.comm.irecv(buf, srcRank, MPI.ANY_TAG))
        else:
            for srcRank in self.expectFrom:
                buf = bytearray(NetworkInterface.irecvBufferSize)
                self.outstandingRecvReqs.append(self.comm.irecv(buf, srcRank, MPI.ANY_TAG))

    def _innerRecv(self, tpl):
        msgType, srcTag, destTag, partTpl = tpl
        logger.debug('msg type %s arrived from %s for %s' % (msgType, srcTag, destTag))
        self.clientIncomingCallbacks[(srcTag.rank, srcTag.lclId, destTag.lclId)](msgType, partTpl)

    def finishRecv(self):
        self.vclock.incr()  # must happen before incoming messages arrive
        logger.debug('%d local messages' % len(self.incomingLclMessages))
        for tpl in self.incomingLclMessages:
            self._innerRecv(tpl)
        self.incomingLclMessages = []
        while True:
            if not self.outstandingRecvReqs:
                break
            if self.deterministic:
                s = MPI.Status()
                msg = MPI.Request.wait(self.outstandingRecvReqs[-1], s)
                logger.debug('netInterface rank %d: wait returned for last idx: tag %s source %s'
                             % (self.comm.rank, s.Get_tag(), s.Get_source()))
                self.outstandingRecvReqs.pop()
                tag = s.Get_tag()
                if tag == NetworkInterface.MPI_TAG_MORE:
                    logger.debug('netInterface rank %d: MORE from %s' %
                                 (self.comm.rank, s.Get_source()))
                    buf = bytearray(NetworkInterface.irecvBufferSize)
                    self.outstandingRecvReqs.append(self.comm.irecv(buf, s.Get_source(),
                                                                    MPI.ANY_TAG))
                else:
                    doneMsg = msg.pop()
                    if doneMsg[0]:
                        self.doneSignalsSeen += 1
                vtm = msg[0]
                #
                # Handle vtime order issues here
                #
                self.vclock.merge(vtm)
                for tpl in msg[1:]:
                    self._innerRecv(tpl)
            else:
                s = MPI.Status()
                idx, msg = MPI.Request.waitany(self.outstandingRecvReqs, s)
                logger.debug('netInterface rank %d: waitany returned for idx %s: tag %s source %s'
                             % (self.comm.rank, idx, s.Get_tag(), s.Get_source()))
                self.outstandingRecvReqs.pop(idx)
                tag = s.Get_tag()
                if tag == NetworkInterface.MPI_TAG_MORE:
                    logger.debug('netInterface rank %d: MORE from %s' %
                                 (self.comm.rank, s.Get_source()))
                    buf = bytearray(NetworkInterface.irecvBufferSize)
                    self.outstandingRecvReqs.append(self.comm.irecv(buf, s.Get_source(),
                                                                    MPI.ANY_TAG))
                else:
                    doneMsg = msg.pop()
                    if doneMsg[0]:
                        self.doneSignalsSeen += 1
                        self.doneMaxCycle = max(self.doneMaxCycle, doneMsg[1])
                vtm = msg[0]
                #
                # Handle vtime order issues here
                #
                self.vclock.merge(vtm)
                for tpl in msg[1:]:
                    self._innerRecv(tpl)
        self.outstandingRecvReqs = []

    def startSend(self):
        vTimeNow = self.vclock.vec
        if self.deterministic:
            l = self.outgoingDict.keys()
            l.sort()
            for destRank in l:
                msgList = self.outgoingDict[destRank][:]
                msgList.sort()
                if destRank == self.comm.rank:
                    # local message
                    for srcTag, destTag, msgType, cargo in msgList:
                        self.incomingLclMessages.append((msgType, srcTag, destTag, cargo))
                else:
                    while msgList:
                        bigCargo = [vTimeNow]
                        for srcTag, destTag, msgType, cargo \
                                in msgList[0:NetworkInterface.maxChunksPerMsg]:
                            bigCargo.append((msgType, srcTag, destTag, cargo))
                        msgList = msgList[NetworkInterface.maxChunksPerMsg:]
                        if msgList:
                            req = self.comm.isend(bigCargo, destRank,
                                                  tag=NetworkInterface.MPI_TAG_MORE)
                        else:
                            bigCargo.extend(self.doneMsg)
                            req = self.comm.isend(bigCargo, destRank,
                                                  tag=NetworkInterface.MPI_TAG_END)
                        self.outstandingSendReqs.append(req)
            self.outgoingDict.clear()
            self.doneMsg = [(False, 0)]  # to avoid accidental re-sends

        else:
            for destRank, msgList in self.outgoingDict.items():
                if destRank == self.comm.rank:
                    # local message
                    for srcTag, destTag, msgType, cargo in msgList:
                        self.incomingLclMessages.append((msgType, srcTag, destTag, cargo))
                else:
                    while msgList:
                        bigCargo = [vTimeNow]
                        for srcTag, destTag, msgType, cargo \
                                in msgList[0:NetworkInterface.maxChunksPerMsg]:
                            bigCargo.append((msgType, srcTag, destTag, cargo))
                        msgList = msgList[NetworkInterface.maxChunksPerMsg:]
                        if msgList:
                            req = self.comm.isend(bigCargo, destRank,
                                                  tag=NetworkInterface.MPI_TAG_MORE)
                        else:
                            bigCargo.extend(self.doneMsg)
                            req = self.comm.isend(bigCargo, destRank,
                                                  tag=NetworkInterface.MPI_TAG_END)
                        self.outstandingSendReqs.append(req)
                        logger.debug('netInterface rank %d sent %s to %s req %s' %
                                     (self.comm.rank, len(bigCargo), destRank, req))
            self.outgoingDict.clear()
            self.doneMsg = [(False, 0)]  # to avoid accidental re-sends

    def finishSend(self):
        sList = []
        for i in range(len(self.outstandingSendReqs)):  # @UnusedVariable
            sList.append(MPI.Status())
        logger.debug('netInterface rank %d enters send waitall' % self.comm.rank)
        MPI.Request.Waitall(self.outstandingSendReqs, statuses=sList)  # @UnusedVariable
        self.outstandingSendReqs = []

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
        cycleNow = self.vclock.vec[self.comm.rank]
        if self.doneSignalSent:
            self.doneMsg = [(False, 0)]
        else:
            self.doneMsg = [(True, cycleNow)]
            self.doneSignalSent = True
            self.doneMaxCycle = max(self.doneMaxCycle, cycleNow)
        return (self.doneSignalsSeen == len(self.expectFrom) and cycleNow >= self.doneMaxCycle + 1)
