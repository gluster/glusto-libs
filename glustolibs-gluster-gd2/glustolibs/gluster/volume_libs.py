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

""" Description: Module for gluster volume related helper functions. """

import time
from glusto.core import Glusto as g
from glustolibs.gluster.lib_utils import form_bricks_list, to_list
from glustolibs.gluster.volume_ops import (volume_create, volume_start,
                                           set_volume_options, get_volume_info,
                                           volume_stop, volume_delete,
                                           volume_info, volume_status,
                                           get_volume_options)


def volume_exists(mnode, volname):
    """Check if volume already exists
    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): Name of the volume.
    Returns:
        bool : True if volume exists. False Otherwise
    """
    volinfo = get_volume_info(mnode, volname)
    if volinfo:
        g.log.info("Volume %s exists", volname)
        return True

    g.log.error("Volume %s doesnot exist", volname)
    return False


def setup_volume(mnode, all_servers_info, volume_config, force=True):
    """Setup Volume with the configuration defined in volume_config
    Args:
        mnode (str): Node on which commands has to be executed
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
        volume_config (dict): Dict containing volume information
        example :
            volume_config = {
                'name': 'testvol',
                'servers': ['server-vm1', 'server-vm2', 'server-vm3',
                            'server-vm4'],
                'voltype': {'type': 'distributed',
                            'dist_count': 4,
                            'transport': 'tcp'},
                'extra_servers': ['server-vm9', 'server-vm10',
                                  'server-vm11', 'server-vm12'],
                'quota': {'limit_usage': {'path': '/', 'percent': None,
                                          'size': '100GB'},
                          'enable': False},
                'uss': {'enable': False},
                }
    Returns:
        bool : True on successful setup. False Otherwise
    """
    dist_count = replica_count = arbiter_count = 0
    # Checking for missing parameters
    for param in ['name', 'servers', 'voltype']:
        if param not in volume_config:
            g.log.error("Unable to get %s info from config", param)
            return False

    # Get volume name
    volname = volume_config['name']

    # Check if the volume already exists
    volume = volume_exists(mnode, volname)
    if not volume:
        g.log.info("Volume %s does not exist in %s", volname, mnode)
        return True

    # Get servers
    servers = volume_config['servers']

    # Get the volume type and values
    volume_type = volume_config['voltype']['type']
    if not volume_type:
        g.log.error("Volume type is not defined in the config")
        return False

    number_of_bricks = 1
    if volume_type == 'distributed':
        if 'dist_count' in volume_config['voltype']:
            dist_count = (volume_config['voltype']['dist_count'])
        else:
            g.log.error("Distribute count not specified in the volume config")
            return False

        number_of_bricks = dist_count

    elif volume_type == 'replicated':
        if 'replica_count' in volume_config['voltype']:
            replica_count = (volume_config['voltype']['replica_count'])
        else:
            g.log.error("Replica count not specified in the volume config")
            return False

        if 'arbiter_count' in volume_config['voltype']:
            arbiter_count = (volume_config['voltype']['arbiter_count'])

        number_of_bricks = replica_count + arbiter_count

    elif volume_type == 'distributed-replicated':
        if 'dist_count' in volume_config['voltype']:
            dist_count = (volume_config['voltype']['dist_count'])
        else:
            g.log.error("Distribute count not specified in the volume config")
            return False

        if 'replica_count' in volume_config['voltype']:
            replica_count = (volume_config['voltype']['replica_count'])
        else:
            g.log.error("Replica count not specified in the volume config")
            return False

        if 'arbiter_count' in volume_config['voltype']:
            arbiter_count = (volume_config['voltype']['arbiter_count'])

        number_of_bricks = dist_count * (replica_count + arbiter_count)

    # get bricks_list
    bricks_list = form_bricks_list(mnode=mnode, volname=volname,
                                   number_of_bricks=number_of_bricks,
                                   servers=servers,
                                   servers_info=all_servers_info)
    if not bricks_list:
        g.log.error("Number_of_bricks is greater than the unused bricks on "
                    "servers")
        return False

    # Create volume
    g.log.info("Force %s", force)
    ret, out, err = volume_create(mnode=mnode, volname=volname,
                                  bricks_list=bricks_list, force=force,
                                  replica_count=replica_count,
                                  arbiter_count=arbiter_count)
    g.log.info("ret %s out %s err %s", ret, out, err)
    if ret:
        g.log.error("Unable to create volume %s", volname)
        return False

    # Start Volume
    ret, _, _ = volume_start(mnode, volname)
    if ret:
        g.log.error("volume start %s failed", volname)
        return False

    # Set all the volume options:
    if 'options' in volume_config:
        volume_options = volume_config['options']
        ret = set_volume_options(mnode=mnode, volname=volname,
                                 options=volume_options)
        if not ret:
            g.log.error("Unable to set few volume options")
            return False
    return True


def cleanup_volume(mnode, volname):
    """deletes snapshots in the volume, stops and deletes the gluster
       volume if given volume exists in gluster and deletes the
       directories in the bricks associated with the given volume
    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.
    Returns:
        bool: True, if volume is deleted successfully
              False, otherwise
    Example:
        cleanup_volume("", "testvol")
    """
    volume = volume_exists(mnode, volname)
    if not volume:
        g.log.info("Volume %s does not exist in %s", volname, mnode)
        return True

    ret, _, _ = g.run(mnode, "glustercli snapshot delete all %s" % volname)
    if ret:
        g.log.error("Failed to delete the snapshots in volume %s", volname)
        return False

    ret, _, _ = volume_stop(mnode, volname)
    if ret:
        g.log.error("Failed to stop volume %s" % volname)
        return False

    ret = volume_delete(mnode, volname)
    if not ret:
        g.log.error("Unable to cleanup the volume %s", volname)
        return False

    return True


def log_volume_info_and_status(mnode, volname):
    """Logs volume info and status
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Returns:
        bool: Returns True if getting volume info and status is successful.
            False Otherwise.
    """
    ret, _, _ = volume_info(mnode, volname)
    if ret:
        g.log.error("Failed to get volume info %s", volname)
        return False

    ret, _, _ = volume_status(mnode, volname)
    if ret:
        g.log.error("Failed to get volume status %s", volname)
        return False

    return True


def verify_all_process_of_volume_are_online(mnode, volname):
    """Verifies whether all the processes of volume are online
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Returns:
        bool: Returns True if all the processes of volume are online.
            False Otherwise.
    """
    # Importing here to avoid cyclic imports
    from glustolibs.gluster.brick_libs import are_bricks_online, get_all_bricks
    # Verify all the  brick process are online
    bricks_list = get_all_bricks(mnode, volname)
    if not bricks_list:
        g.log.error("Failed to get the brick list "
                    "from the volume %s", volname)
        return False

    ret = are_bricks_online(mnode, volname, bricks_list)
    if not ret:
        g.log.error("All bricks are not online of "
                    "the volume %s", volname)
        return False

    # ToDO: Verify all self-heal-daemons are running for non-distribute volumes

    return True


def get_subvols(mnode, volname):
    """Gets the subvolumes in the given volume
    Args:
        volname (str): volume name
        mnode (str): Node on which cmd has to be executed.
    Returns:
        dict: with empty list values for all keys, if volume doesn't exist
        dict: Dictionary of subvols, value of each key is list of lists
            containing subvols
    Example:
        get_subvols("abc.xyz.com", "testvol")
    """

    bricks_path = []
    subvols = {'volume_subvols':[]}
    volinfo = get_volume_info(mnode, volname)
    if volinfo:
        subvol_info = volinfo['subvols']
        for subvol in subvol_info:
            path = []
            for brick in subvol['bricks']:
                path1 = ':'.join([brick['host'], brick['path']])
                path.append(path1)
            bricks_path.append(path)
            subvols['volume_subvols'] = bricks_path
    return subvols


def is_distribute_volume(mnode, volname):
    """Check if volume is a plain distributed volume
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
    Returns:
        bool : True if the volume is distributed volume. False otherwise
        NoneType: None if volume does not exist.
    """
    volume_type_info = get_volume_type_info(mnode, volname)
    if not volume_type_info:
        g.log.error("Unable to check if the volume %s is distribute", volname)
        return False

    return bool(volume_type_info['type'] == 'Distribute')


def get_volume_type_info(mnode, volname):
    """Returns volume type information for the specified volume.
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
    Returns:
        dict : Dict containing the keys, values defining the volume type:
        NoneType: None if volume does not exist or any other key errors.
    """
    volinfo = get_volume_info(mnode, volname)
    if not volinfo:
        g.log.error("Unable to get the volume info for volume %s", volname)
        return None

    volume_type_info = {
        'type': '',
        'replica-count': '',
        'arbiter-count': '',
        'distribute-count': ''
        }
    for key in volume_type_info.keys():
        if key in volinfo:
            volume_type_info[key] = volinfo[key]
        else:
            volume_type_info[key] = None

    return volume_type_info


def get_num_of_bricks_per_subvol(mnode, volname):
    """Returns number of bricks per subvol
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
    Returns:
        dict : Dict containing the keys, values defining
                number of bricks per subvol
        NoneType: None if volume does not exist.
    """
    bricks_per_subvol_dict = {
        'volume_num_of_bricks_per_subvol': None
        }

    subvols_dict = get_subvols(mnode, volname)
    if subvols_dict['volume_subvols']:
        bricks_per_subvol_dict['volume_num_of_bricks_per_subvol'] = (
            len(subvols_dict['volume_subvols'][0]))

    return bricks_per_subvol_dict


def get_replica_count(mnode, volname):
    """Get the replica count of the volume
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
    Returns:
        dict : Dict contain keys, values defining Replica count of the volume.
        NoneType: None if it is parse failure.
    """
    vol_type_info = get_volume_type_info(mnode, volname)
    if not vol_type_info:
        g.log.error("Unable to get the replica count info for the volume %s",
                    volname)
        return None

    return vol_type_info['replica-count']


def enable_and_validate_volume_options(mnode, volname, volume_options_list,
                                       time_delay=1):
    """Enable the volume option and validate whether the option has be
    successfully enabled or not
    Args:
        mnode (str): Node on which commands are executed.
        volname (str): Name of the volume.
        volume_options_list (str|list): A volume option|List of volume options
            to be enabled
        time_delay (int): Time delay between 2 volume set operations
    Returns:
        bool: True when enabling and validating all volume options is
            successful. False otherwise
    """

    volume_options_list = to_list(volume_options_list)

    for option in volume_options_list:
        # Set volume option to 'enable'
        g.log.info("Setting the volume option : %s", )
        ret = set_volume_options(mnode, volname, {option: "on"})
        if not ret:
            return False

        # Validate whether the option is set on the volume
        g.log.info("Validating the volume option : %s to be set to 'enable'",
                   option)
        option_dict = get_volume_options(mnode, volname, option)
        g.log.info("Options Dict: %s", option_dict)
        if not option_dict:
            g.log.error("%s is not enabled on the volume %s", option, volname)
            return False

        if option not in option_dict['name'] or "on" not in option_dict['value']:
            g.log.error("%s is not enabled on the volume %s", option, volname)
            return False

        g.log.info("%s is enabled on the volume %s", option, volname)
        time.sleep(time_delay)

    return True


def form_bricks_list_to_add_brick(mnode, volname, servers, all_servers_info,
                                  replica_count=None,
                                  distribute_count=None):
    """Forms list of bricks to add-bricks to the volume.
    Args:
        mnode (str): Node on which commands has to be executed
        volname (str): volume name
        servers (list): List of servers in the storage pool.
        all_servers_info (dict): Information about all servers.
        example :
            all_servers_info = {
                'abc.lab.eng.xyz.com': {
                    'host': 'abc.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    },
                'def.lab.eng.xyz.com':{
                    'host': 'def.lab.eng.xyz.com',
                    'brick_root': '/bricks',
                    'devices': ['/dev/vdb', '/dev/vdc', '/dev/vdd', '/dev/vde']
                    }
                }
    Kwargs:
        - replica_count : (int)|None.
            Increase the current_replica_count by replica_count
        - distribute_count: (int)|None.
            Increase the current_distribute_count by distribute_count
    Returns:
        list: List of bricks to add if there are enough bricks to add on
            the servers.
        nonetype: None if there are not enough bricks to add on the servers or
            volume doesn't exists or any other failure.
    """

    # Check if volume exists
    if not volume_exists(mnode, volname):
        g.log.error("Volume %s doesn't exists.", volname)
        return None

    # Check if replica_count and distribute_count is given
    if not replica_count and not distribute_count:
        distribute_count = 1

    # Check if the volume has to be expanded by n distribute count.
    num_of_distribute_bricks_to_add = 0
    if distribute_count:
        # Get Number of bricks per subvolume.
        bricks_per_subvol = get_num_of_bricks_per_subvol(mnode, volname)
        num_of_bricks_per_subvol = (
                bricks_per_subvol['volume_num_of_bricks_per_subvol'])

        # Get number of bricks to add.
        if not num_of_bricks_per_subvol:
            g.log.error("Number of bricks per subvol is None. "
                        "Something majorly went wrong on the volume %s",
                        volname)
            return False

        num_of_distribute_bricks_to_add = (num_of_bricks_per_subvol *
                                           distribute_count)

    # Check if the volume has to be expanded by n replica count.
    num_of_replica_bricks_to_add = 0
    if replica_count:
        # Get Subvols
        subvols_info = get_subvols(mnode, volname)

        # Calculate number of bricks to add
        if subvols_info['volume_subvols']:
            num_of_subvols = len(subvols_info['volume_subvols'])

        if not num_of_subvols:
            g.log.error("No Sub-Volumes available for the volume %s."
                        " Hence cannot proceed with add-brick", volname)
            return None

        num_of_replica_bricks_to_add = replica_count * num_of_subvols

    # Calculate total number of bricks to add
    if (num_of_distribute_bricks_to_add != 0 and
            num_of_replica_bricks_to_add != 0):
        num_of_bricks_to_add = (
                num_of_distribute_bricks_to_add +
                num_of_replica_bricks_to_add +
                (distribute_count * replica_count)
            )
    else:
        num_of_bricks_to_add = (
            num_of_distribute_bricks_to_add +
            num_of_replica_bricks_to_add
        )

    # Form bricks list to add bricks to the volume.
    bricks_list = form_bricks_list(mnode=mnode, volname=volname,
                                   number_of_bricks=num_of_bricks_to_add,
                                   servers=servers,
                                   servers_info=all_servers_info)
    if not bricks_list:
        g.log.error("Number of bricks is greater than the unused bricks on "
                    "servers. Hence failed to form bricks list to "
                    "add-brick")
        return None
    return bricks_list



def wait_for_volume_process_to_be_online(mnode, volname, timeout=300):
    """Waits for the volume's processes to be online until timeout
    Args:
        mnode (str): Node on which commands will be executed.
        volname (str): Name of the volume.
    Kwargs:
        timeout (int): timeout value in seconds to wait for all volume
        processes to be online.
    Returns:
        True if the volume's processes are online within timeout,
        False otherwise
    """
    # Adding import here to avoid cyclic imports
    from glustolibs.gluster.brick_libs import wait_for_bricks_to_be_online

    # Wait for bricks to be online
    bricks_online_status = wait_for_bricks_to_be_online(mnode, volname,
                                                        timeout)
    if not bricks_online_status:
        g.log.error("Failed to wait for the volume '%s' processes "
                    "to be online", volname)
        return False

    # ToDo: Wait for self-heal-daemons to be online

    # TODO: Add any process checks here

    g.log.info("Volume '%s' processes are all online", volname)
    return True
