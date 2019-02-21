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

logger = logging.getLogger(__name__)

from netinterface_base import *

try:
    from mpi4py import MPI
    logger.error('Import MPI succeeded')
    if MPI.COMM_WORLD.size > 1:
        from netinterface_mpi import *
        logger.error('implementing the mpi version')
    else:
        from netinterface_dummy import *
        logger.error('Only 1 rank so using the dummy version')
except ImportError:
    logger.error('Import MPI failed; falling back to dummy version')
    from netinterface_dummy import *

