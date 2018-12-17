#  Copyright (C) 2018  Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


import httplib
from glustolibs.gluster.rest import RestClient
from glustolibs.gluster.exceptions import (GlusterApiInvalidInputs)
from glustolibs.gluster.volume_ops import validate_brick


"""This module contains the python glusterd2 brick related api's implementation."""


def add_brick(
        mnode, volname, bricks_list, force=False,
        replica_count=0, arbiter_count=0):
    """Add Bricks specified in the bricks_list to the volume.
    Args:
        mnode (str): None on which the commands are executed.
        volname (str): Name of the volume
        bricks_list (list): List of bricks to be added
    Kwargs:
        force (bool): If this option is set to True, then add brick command
            will get executed with force option. If it is set to False,
            then add brick command will get executed without force option
        **kwargs
            The keys, values in kwargs are:
                - replica_count : (int)
                - arbiter_count : (int)
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err' is of type 'str' and is the status
            error msg of operation else returns None.
    Example:
        add_brick(mnode, volname, bricks_list)
    """
    if len(bricks_list) <= 0:
            raise GlusterApiInvalidInputs("Bricks cannot be empty")

    req_bricks = validate_brick(bricks_list)
    if req_bricks is None:
        raise GlusterApiInvalidInputs("Invalid Brick details, bricks "
                                      "should be in form of "
                                      "<peerid>:<path>")
    # To create a brick dir
    create_brick_dir = {"create-brick-dir": True}

    data = {
            "ReplicaCount" : replica_count,
            "Bricks" : req_bricks,
            "Force" : force,
            "Flags": create_brick_dir
            }

    return RestClient(mnode).handle_request(
            "POST", "/v1/volumes/%s/expand" % volname, httplib.OK, data)
