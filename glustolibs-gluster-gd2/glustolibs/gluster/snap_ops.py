#  Copyright (C) 2018-2019  Red Hat, Inc. <http://www.redhat.com>
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
"""
     Description: Library for gluster snapshot operations.
"""

import json
import httplib
from glusto.core import Glusto as g
from glustolibs.gluster.rest import RestClient
from glustolibs.gluster.volume_ops import volume_start, volume_stop


def snap_create(mnode, volname, snapname, timestamp=False, description=None):
    """Creates snapshot for the given volume.

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        snapname (str): snapshot name

    Kwargs:
        timestamp (bool): If this option is set to True, then
            timestamps will get appended to the snapname. If this option
            is set to False, then timestamps will not be appended to snapname.
        description (str): description for snapshot creation

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        snap_create("abc.com", testvol, testsnap)

    """
    data = {"snapname": snapname, "volname": volname,
            "description": description, "timestamp": timestamp}
    return RestClient(mnode).handle_request("POST", "/v1/snapshots", httplib.CREATED, data)


def snap_activate(mnode, snapname):
    """Activates the given snapshot

    Args:
        mnode (str): Node on which cmd has to be executed.
        snapname (str): snapshot name to be activated

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        snap_activate("abc.com", testsnap)

    """
    return RestClient(mnode).handle_request('POST', "/v1/snapshots/%s/activate"
                                            % snapname, httplib.OK, None)


def snap_deactivate(mnode, snapname):
    """Deactivates the given snapshot

    Args:
        mnode (str): Node on which cmd has to be executed.
        snapname (str): snapshot name to be deactivated

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        snap_deactivate("abc.com", testsnap)

    """
    return RestClient(mnode).handle_request('POST',
                                            "/v1/snapshots/%s/deactivate"
                                            % snapname, httplib.OK, None)


def snap_clone(mnode, snapname, clonename):
    """Clones the given snapshot

    Args:
        mnode (str): Node on which cmd has to be executed.
        snapname (str): snapshot name to be cloned
        clonename (str): clone name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        snap_clone("abc.com", testsnap, clone1)

    """
    data = {"clonename": clonename}
    return RestClient(mnode).handle_request('POST', "/v1/snapshots/%s/clone"
                                            % snapname, httplib.CREATED, data)


def snap_restore(mnode, snapname):
    """Snap restore for the given snapshot

    Args:
        mnode (str): Node on which cmd has to be executed.
        snapname (str): snapshot name to be cloned

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        snap_restore(mnode, testsnap)

    """
    return RestClient(mnode).handle_request('POST', "/v1/snapshots/%s/restore"
                                            % snapname, httplib.CREATED, None)


def snap_restore_complete(mnode, volname, snapname):
    """stops the volume, restore the snapshot and starts the volume

    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        snapname (str): snapshot name

    Returns:
        bool: True on success, False on failure

    Example:
        snap_restore_complete(mnode, testvol, testsnap)

    """

    # Stopping volume before snap restore
    ret, _, _ = volume_stop(mnode, volname)
    if not ret:
        g.log.error("Failed to stop %s volume before restoring snapshot %s
                     in node %s", volname, snapname, mnode)
        return False

    ret, _, _ = snap_restore(mnode, snapname)
    if ret:
    g.log.error("Snapshot %s restore failed on node %s", snapname, mnode)
        return False

    # Starting volume after snap restore
    ret, _, _ = volume_start(mnode, volname)
    if not ret:
        g.log.error("Failed to start volume %s after restoring snapshot %s
                    in node %s" , volname, snapname, mnode)
        return False
    return True


def snap_info(mnode, snapname):
    """Gets the snap info by snapname

    Args:
        mnode (str): Node on which command has to be executed.
        snapname (str): snapshot name

    Returns:
        NoneType: None if command execution fails, parse errors.
        dict: on success.
    """
    return RestClient(mnode).handle_request('GET', "/v1/snapshots/%s"
                                            % snapname, httplib.OK, None)


def snap_list(mnode):
    """Lists the snapshots

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    return RestClient(mnode).handle_request('GET', "/v1/snapshots", httplib.OK, None)


def get_snap_list(mnode):
    """ Lists the snapname

    Args:
        mnode (str): Node on which cmd has to be executed.

    Returns:
        list: List containing the snapname if exists else returns None

    """
    _, out, _ = snap_list(mnode)
    if out:
        output = json.loads(out)
        snap_info = output[0]
        snaps_list = []
        for elem in snap_info['snaps']:
            snaps = elem['snapinfo']['name']
            snaps_list.append(snaps)
        return snaps_list
    return None


def snap_status(mnode, snapname):
    """Get the snap status by snapname

    Args:
        mnode (str): Node on which command has to be executed.
        snapname (str): snapshot name

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    """
    return RestClient(mnode).handle_request('GET', "/v1/snapshots/%s/status"
                                            % snapname, httplib.OK, None)


def snap_delete(mnode, snapname):
    """Deletes the given snapshot

    Args:
        mnode (str): Node on which cmd has to be executed.
        snapname (str): snapshot name to be deleted

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    return RestClient(mnode).handle_request('DELETE', "/v1/snapshots/%s"
                                            % snapname, httplib.DELETE, None)
    # TODO: Few snapshot functions are yet to be automated after it is
    # implemented in gd2

