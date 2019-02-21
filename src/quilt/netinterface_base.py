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

import logging
import numpy as np
from collections import namedtuple

_logger = logging.getLogger(__name__)

class VectorClock(object):
    def __init__(self, commSize, rank, vec=None):
        self.rank = rank
        if vec is None:
            self.vec = np.zeros(commSize, dtype=np.int32)
        else:
            self.vec = np.copy(vec)

    def incr(self):
        self.vec[self.rank] += 1

    def merge(self, foreignVec):
        """ This operation does not include incrementing the local time """
        self.vec = np.maximum(self.vec, foreignVec)

    def max(self):
        return np.amax(self.vec)

    def min(self):
        return np.amin(self.vec)

    def before(self, other):
        """returns True if 'self' is less than the vector clock 'other' """
        return (np.all(np.less_equal(self.vec, other.vec))
                and np.any(np.less(self.vec, other.vec)))

    def after(self, other):
        """returns True if the vector clock 'other' is less than 'self' """
        return (np.all(np.less_equal(other.vec, self.vec))
                and np.any(np.less(other.vec, self.vec)))

    def simultaneous(self, other):
        """returns True if neither vector clock is before the other"""
        return (not self.before(other) and not self.after(other))

    def __str__(self):
        return 'VClock(%s)' % str(self.vec)

    def copy(self):
        return VectorClock(self.vec.shape[0], self.rank, vec=np.copy(self.vec))

_InnerGblAddr = namedtuple('_innerGblAddr', ['rank', 'lclId'])


class GblAddr(_InnerGblAddr):

    def getLclAddr(self):
        return self.lclId

    def getPatchAddr(self):
        if isinstance(self.lclId, tuple):
            return GblAddr(self.rank, self.lclId[0])
        else:
            return GblAddr(self.rank, self.lclId)

    @staticmethod
    def tupleGetPatchAddr(tpl):
        """For those awkward times when the argument is really an _InnerGblAddr"""
        rank = tpl[0]
        lclId = tpl[1]
        if isinstance(lclId, tuple):
            return GblAddr(rank, lclId[0])
        else:
            return GblAddr(rank, lclId)

    def __str__(self):
        if isinstance(self.lclId, tuple):
            return "{0}_{1}_{2}".format(self.rank, self.lclId[0], self.lclId[1])
        else:
            return '%d_%d' % (self.rank, self.lclId)

    def __lt__(self, other):
        return (self.rank < other.rank
                or (self.rank == other.rank and self.lclId < other.lclId))

    def __le__(self, other):
        return self < other or self == other

    def __eq__(self, other):
        return (type(self) == type(other)
                and self.rank == other.rank and self.lclId == other.lclId)

    def __ne__(self, other):
        return (type(self) != type(other) or self.rank != other.rank or self.lclId != other.lclId)

    def __gt__(self, other):
        return (self.rank > other.rank
                or (self.rank == other.rank and self.lclId > other.lclId))

    def __ge__(self, other):
        return self > other or self == other

    def __hash__(self):
        """
        MD: I'm not yet sure if it's a python 3 specific thing,
        but objects that override equivalence functions won't hash with standard object hashing
        Without this, I get a `TypeError: unhashable type: 'GblAddr'`
        More research is required to see if this solution treats the disease or the symptom, however
        """
        return hash((self.rank, self.lclId))
