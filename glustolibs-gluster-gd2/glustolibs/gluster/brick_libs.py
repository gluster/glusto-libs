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
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""
    Description: Module for gluster brick operations.
"""

import json
import random
import time
from glusto.core import Glusto as g
from glustolibs.gluster.peer_ops import get_peer_id
from glustolibs.gluster.volume_ops import get_volume_info, volume_brick_status
from glustolibs.gluster.lib_utils import to_list

def get_all_bricks(mnode, volname):
    """Get list of all the bricks of the specified volume.

    Args:
        mnode (str): Node on which command has to be executed
        volname (str): Name of the volume

    Returns:
        list: List of all the bricks of the volume on Success.
        NoneType: None on failure.
    """

    volinfo = get_volume_info(mnode, volname)
    if volinfo is None:
        g.log.error("Unable to get the volinfo of %s.", volname)
        return None

    all_bricks = []
    for bricks in volinfo['subvols']:
        for brick in bricks['bricks']:
            path = []
            path.append(brick['host'])
            path.append(brick['path'])
            brick = ":".join(path)
            all_bricks.append(brick)
    return all_bricks


def are_bricks_offline(mnode, volname, bricks_list):
    """Verify all the specified list of bricks are offline.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to verify offline status.

    Returns:
        bool : True if all bricks offline. False otherwise.
        NoneType: None on failure in getting volume status
    """

    _rc = True
    offline_bricks_list = []
    _, out, _ = volume_brick_status(mnode, volname)
    out = json.loads(out)
    if not out:
        g.log.error("Unable to check if bricks are offline for the volume %s",
                    volname)
        return None

    for i in range(len(out)):
        status = out[i]['online']
        if not status:
            offline_brick = ':'.join([out[i]['info']['host'],
                                      out[i]['info']['path']])
            offline_bricks_list.append(offline_brick)

    ret = cmp(bricks_list, offline_bricks_list)

    if ret:
        _rc = False

    if not _rc:
        g.log.error("Some of the bricks %s are not offline",
                    offline_bricks_list)
        return _rc

    g.log.info("All the bricks in %s are offline", bricks_list)
    return _rc


def are_bricks_online(mnode, volname, bricks_list):
    """Verify all the specified list of bricks are online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to verify online status.

    Returns:
        bool : True if all bricks online. False otherwise.
        NoneType: None on failure in getting volume status
    """
    _rc = True
    offline_bricks_list = []
    _, out, _ = volume_brick_status(mnode, volname)
    out = json.loads(out)
    if not out:
        g.log.error("Unable to check if bricks are online for the volume %s",
                    volname)
        return None

    for i in range(len(bricks_list)):
        status = out[i]['online']
        if not status:
            offline_brick = ':'.join([out[i]['info']['host'],
                                      out[i]['info']['path']])
            offline_bricks_list.append(offline_brick)
            _rc = False

    if not _rc:
        g.log.error("Some of the bricks %s are not online",
                    offline_bricks_list)
        return False

    g.log.info("All the bricks %s are online", bricks_list)
    return True


def get_offline_bricks_list(mnode, volname):
    """Get list of bricks which are offline.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        list : List of bricks in the volume which are offline.
        NoneType: None on failure in getting volume status
    """
    offline_bricks_list = []
    _, out, _ = volume_brick_status(mnode, volname)
    out = json.loads(out)
    if not out:
        g.log.error("Unable to get offline bricks_list for the volume %s",
                    volname)
        return None

    for i in range(len(out)):
        status = out[i]['online']
        if not status:
            offline_brick = ':'.join([out[i]['info']['host'],
                                      out[i]['info']['path']])
            offline_bricks_list.append(offline_brick)
    return offline_bricks_list


def get_online_bricks_list(mnode, volname):
    """Get list of bricks which are online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Returns:
        list : List of bricks in the volume which are online.
        NoneType: None on failure in getting volume status
    """
    online_bricks_list = []
    _, out, _ = volume_brick_status(mnode, volname)
    out = json.loads(out)
    if not out:
        g.log.error("Unable to get online bricks_list for the volume %s",
                    volname)
        return None

    for i in range(len(out)):
        status = out[i]['online']
        if status:
            online_brick = ':'.join([out[i]['info']['host'],
                                     out[i]['info']['path']])
            online_bricks_list.append(online_brick)
    return online_bricks_list


def wait_for_bricks_to_be_online(mnode, volname, timeout=300):
    """Waits for the bricks to be online until timeout

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.

    Kwargs:
        timeout (int): timeout value in seconds to wait for bricks to be
        online

    Returns:
        True if all bricks are online within timeout, False otherwise
    """
    all_bricks = get_all_bricks(mnode, volname)
    if not all_bricks:
        return False

    counter = 0
    flag = 0
    while counter < timeout:
        status = are_bricks_online(mnode, volname, all_bricks)

        if status:
            flag = 1
            break
        time.sleep(10)
        counter = counter + 10

    if not flag:
        g.log.error("All Bricks of the volume '%s' are not online "
                    "even after %d minutes", volname, timeout/60.0)
        return False
    g.log.info("All Bricks of the volume '%s' are online ", volname)
    return True


def delete_bricks(bricks_list):
    """Deletes list of bricks specified from the brick nodes.

    Args:
        bricks_list (list): List of bricks to be deleted.

    Returns:
        bool : True if all the bricks are deleted. False otherwise.
    """
    _rc = True
    for brick in bricks_list:
        brick_node, brick_path = brick.split(":")
        ret, _, _ = g.run(brick_node, "rm -rf %s | ls %s" % brick_path)
        if ret:
            g.log.error("Unable to delete brick %s on node %s",
                        brick_path, brick_node)
            _rc = False
    return _rc


def bring_bricks_online(mnode, volname, bricks_list,
                        bring_bricks_online_methods=None):
    """Bring the bricks specified in the bricks_list online.

    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
        bricks_list (list): List of bricks to bring them online.

    Kwargs:
        bring_bricks_online_methods (list): List of methods using which bricks
            will be brought online. The method to bring a brick online is
            randomly selected from the bring_bricks_online_methods list.
            By default all bricks will be brought online with
            ['glusterd_restart', 'volume_start_force'] methods.
            If 'volume_start_force' command is randomly selected then all the
            bricks would be started with the command execution. Hence we break
            from bringing bricks online individually

    Returns:
        bool : True on successfully bringing all bricks online.
            False otherwise
    """
    if bring_bricks_online_methods is None:
        bring_bricks_online_methods = ['glusterd_restart',
                                       'volume_start_force']
        bring_brick_online_method = random.choice(bring_bricks_online_methods)

    elif bring_bricks_online_methods = to_list(bring_bricks_online_methods)

    g.log.info("Bringing bricks '%s' online with '%s'",
               bricks_list, bring_bricks_online_methods)

    _rc = True
    failed_to_bring_online_list = []
    if bring_brick_online_method == 'glusterd_restart':
        bring_brick_online_command = "systemctl restart glusterd2"
        for brick in bricks_list:
            brick_node, _ = brick.split(":")
            ret, _, _ = g.run(brick_node, bring_brick_online_command)
            if not ret:
                g.log.error("Unable to restart glusterd on node %s",
                            brick_node)
                _rc = False
                failed_to_bring_online_list.append(brick)
            g.log.info("Successfully restarted glusterd on node %s to "
                       "bring back brick %s online", brick_node, brick)

    elif bring_brick_online_method == 'volume_start_force':
        bring_brick_online_command = ("glustercli volume start %s force" %
                                      volname)
        ret, _, _ = g.run(mnode, bring_brick_online_command)
        if not ret:
            g.log.error("Unable to start the volume %s with force option",
                        volname)
            _rc = False
        g.log.info("Successfully restarted volume %s to bring all "
                   "the bricks '%s' online", volname, bricks_list)
        break
    else:
        g.log.error("Invalid method '%s' to bring brick online",
                    bring_brick_online_method)
        return False

    g.log.info("Waiting for 10 seconds for all the bricks to be online")
    time.sleep(10)
    return _rc


def bring_bricks_offline(bricks_list, volname=None,
                         bring_bricks_offline_methods=None):
    """Bring the bricks specified in the bricks_list offline.

    Args:
        volname (str): Name of the volume
        bricks_list (list): List of bricks to bring them offline.

    Kwargs:
        bring_bricks_offline_methods (list): List of methods using which bricks
            will be brought offline. The method to bring a brick offline is
            randomly selected from the bring_bricks_offline_methods list.
            By default all bricks will be brought offline with
            'service_kill' method.

    Returns:
        bool : True on successfully bringing all bricks offline.
               False otherwise
    """
    if bring_bricks_offline_methods is None:
        bring_bricks_offline_methods = ['service_kill']

    elif bring_bricks_offline_methods = to_list(bring_bricks_offline_methods)

    bricks_list = to_list(bricks_list)
    _rc = True
    failed_to_bring_offline_list = []
    for brick in bricks_list:
        if bring_brick_offline_method == 'service_kill':
            brick_node, brick_path = brick.split(":")
            brick_path = brick_path.replace("/", "-")
            peer_id = get_peer_id(brick_node, brick_node)
            kill_cmd = ("pid=`ps -ef | grep -ve 'grep' | "
                        "grep -e '%s%s.pid' | awk '{print $2}'` && "
                        "kill -15 $pid || kill -9 $pid" %
                        (peer_id, brick_path))
            ret, _, _ = g.run(brick_node, kill_cmd)
            if not ret:
                g.log.error("Unable to kill the brick %s", brick)
                failed_to_bring_offline_list.append(brick)
                _rc = False
        else:
            g.log.error("Invalid method '%s' to bring brick offline",
                        bring_brick_offline_method)
            return False

    if not _rc:
        g.log.error("Unable to bring some of the bricks %s offline",
                    failed_to_bring_offline_list)
        return False

    g.log.info("All the bricks : %s are brought offline", bricks_list)
    return True
