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
from collections import deque
from greenlet import greenlet
from random import randint
import logging
import quilt.weaklist as weaklist

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

try:
    import faulthandler
    faulthandler.enable()
    if sys.excepthook != sys.__excepthook__:
        logger.warning("Warning: 3rd party exception hook is active")
        if sys.excepthook.__name__ == 'apport_excepthook':
            logger.warning("         killing Ubuntu's Apport hook")
            sys.excepthook = sys.__excepthook__
except:
    pass


class Sequencer(object):

    def __init__(self, name, checkpointer=None):
        self._timeQueues = {}
        self._timeNow = 0
        self._name = name
        self.checkpointer = checkpointer
        self._logger = logging.getLogger(__name__ + '.Sequencer')

    def __iter__(self):
        while self._timeQueues:
            todayQueue = self._timeQueues[self._timeNow]
            if todayQueue:
                yield (todayQueue.popleft(), self._timeNow)
            else:
                if self._timeNow in self._timeQueues:
                    del self._timeQueues[self._timeNow]
                self._timeNow += 1
                if self.checkpointer is not None:
                    self.checkpointer.checkpoint(self._timeNow)

    def enqueue(self, agent, whenInfo=0):
        assert isinstance(whenInfo, int), (('%s: cannot enqueue %s: time %s is'
                                                      ' not an integer')
                                                     % (self._name, agent.name, whenInfo))
        assert whenInfo >= self._timeNow, '%s: cannot schedule things in the past' % self._name
        if whenInfo not in self._timeQueues:
            self._timeQueues[whenInfo] = deque()
        self._timeQueues[whenInfo].append(agent)

    def unenqueue(self, agent, expectedWakeTime):
        assert isinstance(expectedWakeTime, int), (('%s: cannot unenqueue %s: time %s'
                                                              ' is not an integer')
                                                             % (self._name, agent.name,
                                                                 expectedWakeTime))
        if expectedWakeTime in self._timeQueues and agent in self._timeQueues[expectedWakeTime]:
            self._timeQueues[expectedWakeTime].remove(agent)
        else:
            wakeTime = self.getAgentWakeTime(agent)
            if wakeTime is not None:
                raise RuntimeError('%s cannot unenqueue %s: enqueued to wake at %s not %s'
                                   % (self._name, agent.name, wakeTime, expectedWakeTime))

    def getAgentWakeTime(self, agent):
        kL = self._timeQueues.keys()
        kL.sort()
        for t in kL:
            if agent in self._timeQueues[t]:
                return t
        return None

    def getTimeNow(self):
        return self._timeNow

    def getNWaitingNow(self):
        if self._timeNow in self._timeQueues:
            return len([a for a in self._timeQueues[self._timeNow] if not a.timeless])
        else:
            return 0

    def getWaitingCensus(self, time=None):
        if time is None:
            time = self._timeNow
        if time in self._timeQueues:
            censusDict = {}
            for a in self._timeQueues[time]:
                nm = type(a).__name__
                if nm in censusDict:
                    censusDict[nm] += 1
                else:
                    censusDict[nm] = 1
            return censusDict
        else:
            return {}

    def bumpTime(self):
        """
        Move time forward by a day, shifting all agents from the old 'today' queue to the new one.

        Normally, all of the remaining agents in the 'today' queue will be timeless when this
        method is called.
        """
        self._logger.info('%s: bump time %s -> %s' % (self._name, self._timeNow, self._timeNow+1))
        oldDay = self._timeQueues[self._timeNow]
        del self._timeQueues[self._timeNow]
        self._timeNow += 1
        if self.checkpointer is not None:
            self.checkpointer.checkpoint(self._timeNow)
        if self._timeNow not in self._timeQueues:
            self._timeQueues[self._timeNow] = deque()
        self._timeQueues[self._timeNow].extend(oldDay)

    def doneWithToday(self):
        """
        If the only agents still in today's sequencer loop are marked 'timeless', this
        will return True; otherwise False.  Note that this condition can be reversed if
        a new agent is inserted into today's loop.
        """
        for iact in Interactant.getLiveList():
            if (iact._lockingAgent is not None and iact._lockingAgent.timeless
                    and iact.getNWaiting()):
                self._logger.debug('doneWithToday is false because %s has %d waiting: %s' %
                                   (iact, iact.getNWaiting(), iact.getWaitingDetails()))
                return False
        return all([a.timeless for a in self._timeQueues[self._timeNow]])


class Agent(greenlet):
    def __init__(self, name, ownerLoop, debug=False):
        self.name = name
        self.ownerLoop = ownerLoop
        self.timeless = False
        self.debug = debug

    def run(self, startTime):
        raise RuntimeError('Derived class must subclass this method!')

    def __getstate__(self):
        return {'name': self.name, 'timeless': self.timeless,
                'debug': self.debug}

    def __setstate__(self, stateDict):
        for k, v in stateDict.items():
            setattr(self, k, v)

    def sleep(self, deltaTime):
        return self.ownerLoop.sleep(self, deltaTime)

    def __str__(self):
        return '<%s>' % self.name

    def kill(self):
        """
        Cause this greenlet to throw GreenletExit.  If it is not the current greenlet,
        this greenlet's parent is set to the current greenlet before throwing the exception,
        causing execution to return to the current (calling) greenlet after the exception
        is thrown.  If the greenlet is the current greenlet, execution passes to its parent.
        """
        if self != greenlet.getcurrent():
            self.parent = greenlet.getcurrent()
        self.throw()

    def nextWakeTime(self):
        """
        Returns the time at which the agent is next expected to wake, or None if it is not
        scheduled.
        """
        return self.ownerLoop.sequencer.getAgentWakeTime(self)


class Interactant(object):
    counter = 0
    _liveInstances = weaklist.WeakList()

    @classmethod
    def getLiveList(cls):
        return cls._liveInstances

    def __init__(self, name, ownerLoop, debug=False):
        self._name = name
        self._ownerLoop = ownerLoop
        self._lockingAgent = None
        self._lockQueue = []
        self._debug = debug
        self._nEnqueued = 0  # counts only things which are not 'timeless'
        self._liveInstances.append(self)
        self.id = Interactant.counter
        Interactant.counter += 1

    def getInfo(self):
        """Used by derived classes to customize sharable information"""
        return self._name

    def getNWaiting(self):
        """This returns the count of waiting agents for which timeless is false"""
        return self._nEnqueued

    def getWaitingDetails(self):
        """Returns a dict of typeName:nOfThisType entries"""
        result = {}
        for a in self._lockQueue:
            nm = type(a).__name__
            if nm in result:
                result[nm] += 1
            else:
                result[nm] = 1
        return result

    def __str__(self):
        return '<%s>' % self._name

    def lock(self, lockingAgent, debug=False):
        """
        Agents always lock interactants before modifying their state.  This can be thought of as
        'docking with' the interactant.  Only one agent can hold a lock for a normal interactant
        and be active; any agent that subsequently tries to lock the same interactant will be
        suspended until the first agent unlocks the interactant.  (However, see MultiInteractant).
        Thus interactants can serve as queues; agents can enqueue themselves by locking the
        interactant and some other agent can modify them while they are in the locked state.
        """
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        if ((self._lockingAgent is None and not self._lockQueue)
                or self._lockingAgent == lockingAgent):
            self._lockingAgent = lockingAgent
            if self._debug or lockingAgent.debug:
                logger.debug('%s fast lock of %s' % (lockingAgent, self._name))
            return timeNow
        else:
            assert lockingAgent == greenlet.getcurrent(), 'Agents may not lock other agents'
            self._lockQueue.append(lockingAgent)
            if not lockingAgent.timeless:
                self._nEnqueued += 1
            if self._debug or lockingAgent.debug:
                logger.debug('%s slow lock of %s (%d in queue)' %
                             (lockingAgent, self._name, self._nEnqueued))
            timeNow = self._ownerLoop.switch('%s is %d in %s queue' %
                                             (lockingAgent, len(self._lockQueue), self._name))
            return timeNow

    def unlock(self, oldLockingAgent):
        """
        This method will typically be called by an active agent which holds a lock on the
        interactant.  The lock is broken, causing the first agent which is suspended waiting
        for a lock to become active.
        """
        assert oldLockingAgent == greenlet.getcurrent(), ('%s unlock of %s with current thread %s'
                                                          % (self._name, oldLockingAgent.name,
                                                             greenlet.getcurrent().name))
        if self._lockingAgent != oldLockingAgent:
            raise RuntimeError('%s is not the lock of %s' % (oldLockingAgent, self._name))
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        if self._lockQueue:
            newAgent = self._lockQueue.pop(0)
            if not newAgent.timeless:
                self._nEnqueued -= 1
            if self._debug:
                logger.debug('%s unlock of %s awakens %s (%d still in queue)' %
                             (self._name, oldLockingAgent, newAgent, self._nEnqueued))
            self._lockingAgent = newAgent
            self._ownerLoop.sequencer.enqueue(newAgent, timeNow)
            self._ownerLoop.sequencer.enqueue(oldLockingAgent, timeNow)
            timeNow = self._ownerLoop.switch("%s and %s enqueued" % (newAgent, oldLockingAgent))
        else:
            if self._debug:
                logger.debug('%s fast unlock of %s' % (self._name, oldLockingAgent))
            self._lockingAgent = None
        return timeNow

    def awaken(self, agent):
        """
        The agent is expected to be sleeping in this interactant's _lockQueue.  Calling
        awaken(agent) removes the agent from _lockQueue and re-inserts it into the
        main loop, so that it will resume running in its turn.  Essentially, the agent
        has been unlocked from the interactant in such a way that it will never become
        active in a locked state.  The agent which calls awaken on a locked agent does
        not yield its thread; the awakened agent simply joins the queue to become active
        in its turn.  It is an error to awaken an agent which is not suspended in the
        interactant's wait queue.
        """
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        if agent not in self._lockQueue:
            raise RuntimeError("%s does not hold %s in its lock queue; cannot awaken" %
                               (self._name, agent.name))
        self._lockQueue.remove(agent)
        if not agent.timeless:
            self._nEnqueued -= 1
        if self._debug:
            logger.debug('%s removes %s from lock queue and awakens it (%d still in queue)' %
                         (self._name, agent.name, self._nEnqueued))
        self._ownerLoop.sequencer.enqueue(agent, timeNow)
        return agent

    def suspend(self, agent):
        """
        The agent is expected to be live and awake but not locked by the current Interactant,
        and the agent must not be the current thread.  This means that the agent must be waiting
        to execute in the sequencer queue.  The agent must be scheduled to run 'now' rather than
        at a future time.  This method removes the agent from the sequencer queue and inserts
        it into the current Interactant's lockQueue, preventing the activation of the agent.
        Suspending and then immediately awakening an agent returns it to its initial state,
        as does awakening and then immediately suspending an agent.
        """
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        if self.isLocked(agent):
            raise RuntimeError("%s is locked by %s; cannot suspend" % (self._name, agent.name))
        self._ownerLoop.sequencer.unenqueue(agent, timeNow)
        self._lockQueue.append(agent)
        if not agent.timeless:
            self._nEnqueued += 1
        if self._debug:
            logger.debug('%s suspends %s and adds to lock queue (%d still in queue)' %
                         (self._name, agent.name, self._nEnqueued))
        return agent

    def isLocked(self, agent):
        """
        Returns True if this interactant is currently locked by the given agent, whether the
        agent is active or has been suspended in the interactant's lock wait queue.  It is
        unlikely that a scenario will arise in which an agent will ever have to test whether
        it holds a lock, but the method exists for completeness of the API.
        """
        return (self._lockingAgent == agent or agent in self._lockQueue)


class MultiInteractant(Interactant):
    """
    A MultiInteractant functions like a generic Interactant, except that more than one
    agent can lock it simultaneously and yet remain active.
    """

    def __init__(self, name, count, ownerLoop, debug=False):
        """
        Create an interactant that can simulaneously hold locks for 'count' active
        agents.
        """
        Interactant.__init__(self, name, ownerLoop, debug)
        self._nLocks = count
        self._lockingAgentSet = set()
        self._debug = debug

    def lock(self, lockingAgent):
        """
        Works like the lock() method of a standard Interactant, except that the first
        'count' agents to lock the interactant remain active.
        """
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        if lockingAgent in self._lockingAgentSet:
            if self._debug or lockingAgent.debug:
                logger.debug('%s already locked by %s' % (self._name, lockingAgent))
            return timeNow
        elif len(self._lockingAgentSet) < self._nLocks:
            self._lockingAgentSet.add(lockingAgent)
            if self._debug or lockingAgent.debug:
                logger.debug('%s fast locked by %s' % (self._name, lockingAgent))
            return timeNow
        else:
            assert lockingAgent == greenlet.getcurrent(), 'Agents may not lock other agents'
            self._lockQueue.append(lockingAgent)
            if not lockingAgent.timeless:
                self._nEnqueued += 1
            if self._debug or lockingAgent.debug:
                logger.debug('%s slow lock by %s (%d in queue)' %
                             (self._name, lockingAgent, self._nEnqueued))
            if lockingAgent == greenlet.getcurrent():
                timeNow = self._ownerLoop.switch('%s is %d in %s queue' %
                                                 (lockingAgent, len(self._lockQueue), self._name))
            return timeNow

    def unlock(self, oldLockingAgent):
        assert oldLockingAgent == greenlet.getcurrent(), ('%s unlock of %s with current thread %s'
                                                          % (self._name, oldLockingAgent.name,
                                                             greenlet.getcurrent().name))
        if oldLockingAgent not in self._lockingAgentSet:
            raise RuntimeError('%s is not a lock of %s' % (oldLockingAgent, self._name))
        timeNow = self._ownerLoop.sequencer.getTimeNow()
        self._lockingAgentSet.remove(oldLockingAgent)
        if self._lockQueue:
            newAgent = self._lockQueue.pop(0)
            if not newAgent.timeless:
                self._nEnqueued -= 1
            if self._debug:
                logger.debug('%s unlock of %s awakens %s (%d still in queue)' %
                             (self._name, oldLockingAgent, newAgent, self._nEnqueued))
            self._lockingAgentSet.add(newAgent)
            self._ownerLoop.sequencer.enqueue(newAgent, timeNow)
            self._ownerLoop.sequencer.enqueue(oldLockingAgent, timeNow)
            timeNow = self._ownerLoop.switch("%s and %s enqueued" % (newAgent, oldLockingAgent))
        else:
            if self._debug:
                logger.debug('%s fast unlock of %s' % (self._name, oldLockingAgent))
        return timeNow

    def isLocked(self, agent):
        return (agent in self._lockingAgentSet or agent in self._lockQueue)

    def __str__(self):
        return '<%s (%d of %d)>' % (self._name, len(self._lockingAgentSet), self._nLocks)

    @property
    def nFree(self):
        return self._nLocks - len(self._lockingAgentSet)

    def getLiveLockedAgents(self):
        return list(self._lockingAgentSet)
    
    @property
    def lockingAgentSet(self):
        return self._lockingAgentSet


def _clockAgentBreakHook(clockAgent):
    """
    This routine exists so that software above this layer (e.g. 'patches') can substitute
    different loop-breaking behavior in MainLoop.ClockAgent .

    The default version yields the thread to the main loop.
    """
    return clockAgent.sleep(0)  # yield thread


class MainLoop(greenlet):
    class ClockAgent(Agent):
        def __init__(self, ownerLoop):
            Agent.__init__(self, 'ClockAgent', ownerLoop)
            self.timeless = True

        def run(self, timeNow):
            while True:
                if not self.ownerLoop.dateFrozen:
                    if self.ownerLoop.sequencer.doneWithToday():
                        self.ownerLoop.sequencer.bumpTime()
                newTimeNow = _clockAgentBreakHook(self)
                for cb in self.ownerLoop.perTickCallbacks:
                    cb(self, timeNow, newTimeNow)
                if newTimeNow != timeNow:
                    for cb in self.ownerLoop.perDayCallbacks:
                        cb(self.ownerLoop, newTimeNow)
                    timeNow = newTimeNow

    @staticmethod
    def everyEventCB(loop, timeNow):
        loop.counter += 1
        if loop.counter > loop.safety:
            loop.logger.info('%s: safety exit' % loop.name)
            loop.parent.switch(loop.counter)
            loop.counter = 0

    @staticmethod
    def everyDayCB(loop, timeNow):
        loop.logger.debug('%s: time is now %s' % (loop.name, timeNow))

    def __init__(self, name=None, safety=None, checkpointer=None):
        self.newAgents = [MainLoop.ClockAgent(self)]
        self.perTickCallbacks = []
        self.perEventCallbacks = []
        self.perDayCallbacks = []
        self.safety = safety  # After how many ticks to bail, if any
        assert safety is None or isinstance(safety, int)
        if name is None:
            self.name = 'MainLoop'
        else:
            self.name = name
        self.sequencer = Sequencer(self.name + ".Sequencer", checkpointer)
        self.dateFrozen = False
        self.counter = 0
        self.addPerDayCallback(MainLoop.everyDayCB)
        if self.safety is not None:
            self.addPerEventCallback(MainLoop.everyEventCB)
        self.stopNow = False
        self.logger = logging.getLogger(__name__ + '.MainLoop')

    def stopRunning(self):
        self.stopNow = True

    def addAgents(self, agentList):
        assert all([a.ownerLoop == self for a in agentList]), \
            "%s: Tried to add a foreign agent!" % self.name
        self.newAgents.extend(agentList)

    def addPerDayCallback(self, cb):
        self.perDayCallbacks.append(cb)

    def addPerTickCallback(self, cb):
        self.perTickCallbacks.append(cb)

    def addPerEventCallback(self, cb):
        self.perEventCallbacks.append(cb)

    def freezeDate(self):
        self.dateFrozen = True

    def unfreezeDate(self):
        self.dateFrozen = False

    def run(self):
        for a in self.newAgents:
            a.parent = self  # so dead agents return here
            self.sequencer.enqueue(a)
        self.newAgents = []
        logDebug = self.logger.isEnabledFor(logging.DEBUG)
        for agent, timeNow in self.sequencer:
            if logDebug:
                self.logger.debug('%s Stepping %s at %d' % (self.name, agent, timeNow))
            for cb in self.perEventCallbacks:
                cb(self, timeNow)
            reply = agent.switch(timeNow)  # @UnusedVariable
            if logDebug:
                self.logger.debug('Stepped %s at %d; reply was %s' % (agent, timeNow, reply))
            if self.stopNow:
                break
        return '%s exiting' % self.name

    def sleep(self, agent, nDays):
        assert isinstance(nDays, int), 'nDays should be an integer'
        assert nDays >= 0, 'No sleeping for negative time'
        self.sequencer.enqueue(agent, self.sequencer.getTimeNow() + nDays)
        return self.switch('%s: %s sleep %d days' % (self.name, agent, nDays))

    def printCensus(self, tickNum=None):
        if tickNum is None:
            print('%s: Census at time %s:' % (self.name, self.sequencer.getTimeNow()))
        else:
            print('%s: Census at tick %s date %s:' %
                  (self.name, tickNum, self.sequencer.getTimeNow()))
        censusDict = {}
        for iact in Interactant.getLiveList():
            for k, v in iact.getWaitingDetails().items():
                if k in censusDict:
                    censusDict[k] += v
                else:
                    censusDict[k] = v
        print('    interactants contain: %s' % censusDict)
        print('    main loop live agents: %s' % self.sequencer.getWaitingCensus())
        print('    main loop tomorrow: %s' %
              self.sequencer.getWaitingCensus(self.sequencer.getTimeNow() + 1))

    def __str__(self):
        return '<%s>' % self.name


def describeSelf():
    print("This main provides diagnostics. -v and -d for verbose and debug respectively.")


def main():
    "This is a simple test routine."
    global verbose, debug

    mainLoop = MainLoop(safety=10000)

    interactants = [Interactant(nm, mainLoop) for nm in ['SubA', 'SubB', 'SubC']]

    class TestAgent(Agent):
        def run(self, startTime):
            timeNow = startTime
            while True:
                print('%s new iter' % self)
                fate = randint(0, len(interactants))
                if fate == len(interactants):
                    print('no lock for %s at %s' % (self.name, timeNow))
                    timeNow = self.sleep(0)  # yield thread
                else:
                    timeNow = interactants[fate].lock(self)
                    timeNow = self.sleep(1)
                    timeNow = interactants[fate].unlock(self)
            return('%s is exiting' % self)

    for a in sys.argv[1:]:
        if a == '-v':
            verbose = True
        elif a == '-d':
            debug = True
        else:
            describeSelf()
            sys.exit('unrecognized argument %s' % a)

    allAgents = []
    for i in range(20000):
        allAgents.append(TestAgent('Agent_%d' % i, mainLoop))

    mainLoop.addAgents(allAgents)
    mainLoop.switch()
    print('all done')

############
# Main hook
############

if __name__ == "__main__":
    main()
