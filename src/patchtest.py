#! /usr/bin/env python

_rhea_svn_id_ = "$Id$"

"""
This provides a simple test routine for patches.
"""

import sys
from greenlet import greenlet
from random import randint

import patches


class TestAgent(patches.Agent):
    def __init__(self, name, ownerPatch, debug=False):
        patches.Agent.__init__(self, name, ownerPatch, debug)

    def run(self, startTime):
        timeNow = startTime
        while True:
            fate = randint(0, len(self.patch.interactants))
            if fate == len(self.patch.interactants):
                whichGate = randint(0, len(self.patch.gates)-1)
                gate = self.patch.gates[whichGate]
                if self.debug:
                    print '%s is jumping to %s at %s' % (self.name, whichGate, timeNow)
                timeNow = gate.lock(self)
                timeNow = gate.unlock(self)  # but it's no longer going to be there
            else:
                timeNow = self.patch.interactants[fate].lock(self)
                if self.debug:
                    print 'progress for %s' % self.name
                timeNow = self.sleep(1)
                timeNow = self.patch.interactants[fate].unlock(self)
        if self.debug:
            return '%s is exiting at %s' % (self, timeNow)

    def __getstate__(self):
        d = patches.Agent.__getstate__(self)
        return d

    def __setstate__(self, stateDict):
        patches.Agent.__setstate__(self, stateDict)


class TestPatch(patches.Patch):
    def __init__(self, group, name=None):
        patches.Patch.__init__(self, group, name)
        self.interactants = []
        self.gates = []

    def setInteractants(self, interactantList):
        self.interactants = interactantList

    def addGateTo(self, destTag):
        g = patches.Patch.addGateTo(self, destTag)
        self.gates.append(g)


def greenletTrace(event, args):
    if event == 'switch':
        origin, target = args
        # Handle a switch from origin to target.
        # Note that callback is running in the context of target
        # greenlet and any exceptions will be passed as if
        # target.throw() was used instead of a switch.
        print 'TRACE switch %s -> %s (parent %s)' % (origin, target, target.parent)
        return
    if event == 'throw':
        origin, target = args
        # Handle a throw from origin to target.
        # Note that callback is running in the context of target
        # greenlet and any exceptions will replace the original, as
        # if target.throw() was used with the replacing exception.
        print 'TRACE throw %s -> %s' % (origin, target)
        return


def describeSelf():
    print """
    This main provides diagnostics. -t, -v and -d for trace, verbose and debug respectively.
    """


def main():
    trace = False
    verbose = False
    debug = False

    for a in sys.argv[1:]:
        if a == '-v':
            verbose = True
        elif a == '-d':
            debug = True
        elif a == '-t':
            trace = True
        else:
            describeSelf()
            sys.exit('unrecognized argument %s' % a)

    comm = patches.getCommWorld()
    rank = comm.rank

    if trace:
        greenlet.settrace(greenletTrace)

    patchGroup = patches.PatchGroup('PatchGroup_%d' % rank, comm)
    nPatches = 2
#     if rank == 0:
#         nPatches = 2
    for j in xrange(nPatches):
        print 'rank %d point 0' % rank

        patch = TestPatch(patchGroup)
        print 'rank %d point 0b' % rank

        patch.setInteractants([patches.Interactant('%s_%d_%d' % (nm, rank, j), patch)
                               for nm in ['SubA', 'SubB', 'SubC']])

        leftFriend = patches.GblAddr((rank-1) % comm.size, (j+1) % nPatches)
        leftGateEntrance = patch.addGateTo(leftFriend)  # @UnusedVariable
        leftGateExit = patch.addGateFrom(leftFriend)  # @UnusedVariable

        rightFriend = patches.GblAddr((rank+1) % comm.size, (j-1) % nPatches)
        rightGateEntrance = patch.addGateTo(rightFriend)  # @UnusedVariable
        rightGateExit = patch.addGateFrom(rightFriend)  # @UnusedVariable

        allAgents = []
        for i in xrange(100):
            allAgents.append(TestAgent('Agent_%d_%d_%d' % (rank, j, i),
                                       patch, debug=debug))

        patch.addAgents(allAgents)
        patchGroup.addPatch(patch)
    print 'Rank %s reaches barrier' % rank
    patchGroup.barrier()
    print 'Rank %s leaves barrier' % rank
    patchGroup.switch()
    print '%d all done (from main)' % rank


############
# Main hook
############

if __name__ == "__main__":
    main()
