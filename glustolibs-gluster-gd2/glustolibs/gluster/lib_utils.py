#  Copyright (C) 2019  Red Hat, Inc. <http://www.redhat.com>
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
    Description: Helper library for gluster modules.
"""

import os
import random
import copy
import datetime
import socket
from glusto.core import Glusto as g
from glustolibs.gluster.mount_ops import create_mount_objs
from glustolibs.gluster.expections import ConfigError


def inject_msg_in_logs(nodes, log_msg, list_of_dirs=None, list_of_files=None):
    """Injects the message to all log files under all dirs specified on nodes.
    Args:
        nodes (str|list): A server|List of nodes on which message has to be
            injected to logs
        log_msg (str): Message to be injected
        list_of_dirs (list): List of dirs to inject message on log files.
        list_of_files (list): List of files to inject message.
    Returns:
        bool: True if successfully injected msg on all log files.
    """
    if isinstance(nodes, str):
        nodes = [nodes]

    if list_of_dirs is None:
        list_of_dirs = ""

    if isinstance(list_of_dirs, list):
        list_of_dirs = ' '.join(list_of_dirs)

    if list_of_files is None:
        list_of_files = ''

    if isinstance(list_of_files, list):
        list_of_files = ' '.join(list_of_files)

    inject_msg_on_dirs = ""
    inject_msg_on_files = ""
    if list_of_dirs:
        inject_msg_on_dirs = (
            "for dir in %s ; do "
            "for file in `find ${dir} -type f -name '*.log'`; do "
            "echo \"%s\" >> ${file} ; done ;"
            "done; " % (list_of_dirs, log_msg))
    if list_of_files:
        inject_msg_on_files = ("for file in %s ; do "
                               "echo \"%s\" >> ${file} ; done; " %
                               (list_of_files, log_msg))

    cmd = inject_msg_on_dirs + inject_msg_on_files

    results = g.run_parallel(nodes, cmd)

    _rc = True
    # Check for return status
    for host in results:
        ret, _, _ = results[host]
        if ret != 0:
            g.log.error("Failed to inject log message '%s' in dirs '%s', "
                        "in files '%s',  on node'%s'",
                        log_msg, list_of_dirs, list_of_files, host)
            _rc = False
    return _rc


def inject_msg_in_gluster_logs(msg, servers, clients,
                               mount_type,
                               server_gluster_logs_dirs,
                               server_gluster_logs_files,
                               client_gluster_logs_dirs,
                               client_gluster_logs_files):

    """Inject all the gluster logs on servers, clients with msg
    Args:
        msg (str): Message string to be injected
    Returns:
        bool: True if injecting msg on the log files/dirs is successful.
              False Otherwise.
    """
    _rc = True
    # Inject msg on server gluster logs
    ret = inject_msg_in_logs(servers, log_msg=msg,
                             list_of_dirs=server_gluster_logs_dirs)
    if not ret:
        _rc = False

    if mount_type is not None and "glusterfs" in mount_type:
        ret = inject_msg_in_logs(clients, log_msg=msg,
                                 list_of_dirs=client_gluster_logs_dirs,
                                 list_of_files=client_gluster_logs_files)
        if not ret:
            _rc = False
    return _rc


def get_ip_from_hostname(nodes):
    """Returns list of IP's for the list of nodes in order.
    Args:
        nodes(list|str): List of nodes hostnames
    Returns:
        list: List of IP's corresponding to the hostnames of nodes.
    """
    nodes_ips = []
    nodes = to_list(nodes)
    for node in nodes:
        try:
            ip = socket.gethostbyname(node)
        except socket.gaierror as e:
            g.log.error("Failed to get the IP of Host: %s : %s", node,
                        e.strerror)
            ip = None
        nodes_ips.append(ip)
    return nodes_ips


def set_conf_entity(entity_name):
    """Set the value of the entity
    Args:
        entity_name (str) : Value of entity to be set
    Returns:
        Value of the entity
    """
    entity = g.config.get(entity_name)
    if not entity:
        raise ConfigError("'%s' not defined in the global config" % entity_name)
    return entity


def configure_volumes(servers, volume_type):
    """Defines the volume configurations.
    Args:
        servers(list) : List of servers
        volume_type(str) : Type of volume which will be created
    Returns:
        default_volume_type_config(dict) : Volume type configuration
        volume_create_force(bool) : Volume with force option
        volume(dict): Volume configuration
        volname(str): Volume name
        voltype(str): Volume type
    """
    # Defining default volume_types configuration.
    default_volume_type_config = {
        'distributed': {
            'type': 'distributed',
            'dist_count': 4,
            'transport': 'tcp'
            },
        'replicated': {
            'type': 'replicated',
            'replica_count': 2,
            'arbiter_count': 1,
            'transport': 'tcp'
            },
        'distributed-replicated': {
            'type': 'distributed-replicated',
            'dist_count': 2,
            'replica_count': 3,
            'transport': 'tcp'
            }
        }

    # Check if default volume_type configuration is provided in
    # config yml
    if g.config.get('gluster')['volume_types']:
        default_volume_type_from_config = (g.config['gluster']['volume_types'])

        for vol_type in default_volume_type_from_config.keys():
            if default_volume_type_from_config[vol_type]:
                if vol_type in default_volume_type_config:
                    default_volume_type_config[volume_type] = (
                            default_volume_type_from_config[vol_type])

    # Create Volume with force option
    volume_create_force = False
    if g.config.get('gluster')['volume_create_force']:
        volume_create_force = (g.config['gluster']['volume_create_force'])

    # Get the volume configuration.
    volume = {}
    if volume_type:
        found_volume = False
        if g.config.get('gluster')['volumes']:
            for volume in g.config['gluster']['volumes']:
                if volume['voltype']['type'] == volume_type:
                    volume = copy.deepcopy(volume)
                    found_volume = True
                    break

        if found_volume:
            if 'name' not in volume:
                volume['name'] = 'testvol_%s' % volume_type

            if 'servers' not in volume:
                volume['servers'] = servers

        if not found_volume:
            try:
                if g.config['gluster']['volume_types'][volume_type]:
                    volume['voltype'] = (g.config['gluster']['volume_types'][volume_type])
            except KeyError:
                try:
                    volume['voltype'] = (default_volume_type_config
                                         [volume_type])
                except KeyError:
                    raise ConfigError("Unable to get configs of volume "
                                      "type: %s", volume_type)
            volume['name'] = 'testvol_%s' % volume_type
            volume['servers'] = servers

        # Define Volume Useful Variables.
        volname = volume['name']
        voltype = volume['voltype']['type']
        servers = volume['servers']
        mnode = servers[0]
    return (default_volume_type_config, volume_create_force,
            volume, voltype, volname, mnode)


def configure_mounts(mnode, volname, mount_type, all_clients_info):
    """Defines the mount configurations.
    Args:
        mnode(str): Node on which volume should be mounted
        volname(str): Name of the volume
        mount_type(list): Defines the mount type
        all_clients_info(dict): Dict of clients information
    Returns:
        mounts_dict_list(list): List of the mount informations
        mounts(str) : GlusterMount instance
    """
    # Get the mount configuration
    mounts = []
    if mount_type:
        mounts_dict_list = []
        found_mount = False
        if g.config.get('gluster')['mounts']:
            for mount in g.config['gluster']['mounts']:
                if mount['protocol'] == mount_type:
                    temp_mount = {}
                    temp_mount['protocol'] = mount_type
                    if 'volname' in mount and mount['volname']:
                        if mount['volname'] == volname:
                            temp_mount = copy.deepcopy(mount)
                        else:
                            continue
                    else:
                        temp_mount['volname'] = volname
                    if ('server' not in mount or
                            (not mount['server'])):
                        temp_mount['server'] = mnode
                    else:
                        temp_mount['server'] = mount['server']
                    if ('mountpoint' not in mount or
                            (not mount['mountpoint'])):
                        temp_mount['mountpoint'] = (os.path.join(
                            "/mnt", '_'.join([volname,
                                              mount_type])))
                    else:
                        temp_mount['mountpoint'] = mount['mountpoint']
                    if ('client' not in mount or
                            (not mount['client'])):
                        temp_mount['client'] = (
                            all_clients_info[
                                random.choice(
                                        all_clients_info.keys())]
                            )
                    else:
                        temp_mount['client'] = mount['client']
                    if 'options' in mount and mount['options']:
                        temp_mount['options'] = mount['options']
                    else:
                        temp_mount['options'] = ''
                    mounts_dict_list.append(temp_mount)
                    found_mount = True

        if not found_mount:
            for client in all_clients_info.keys():
                mount = {
                        'protocol': mount_type,
                         'server': mnode,
                         'volname': volname,
                         'client': all_clients_info[client],
                         'mountpoint': (os.path.join(
                            "/mnt", '_'.join([volname, mount_type]))),
                         'options': ''
                        }
                mounts_dict_list.append(mount)
        mounts = create_mount_objs(mounts_dict_list)

        # Defining clients from mounts.
        clients = []
        for mount in mounts_dict_list:
            clients.append(mount['client']['host'])
        clients = list(set(clients))

        return clients, mounts_dict_list, mounts


def configure_logs():
    """Defines the gluster log information.
    Returns:
        server_gluster_logs_dirs(list) : List of server logs dirs
        server_gluster_logs_files(list) : List of server logs files
        client_gluster_logs_dirs(list) : List of client logs dirs
        client_gluster_logs_files(list) : List of client logs files
        glustotest_run_id(str) : Time the test run
    """
    # Gluster Logs info
    server_gluster_logs_dirs = ["/var/log/glusterd2/glusterd2.log"]
    server_gluster_logs_files = []
    if g.config.get("gluster")['server_gluster_logs_info']['dirs']:
        server_gluster_logs_dirs = (
                 g.config['gluster']['server_gluster_logs_info']['dirs'])

    if g.config.get("gluster")['server_gluster_logs_info']['files']:
        server_gluster_logs_files = (
                g.config['gluster']['server_gluster_logs_info']['files'])

    client_gluster_logs_dirs = ["/var/log/glusterd2/glusterd2.log"]
    client_gluster_logs_files = ["/var/log/glusterd2/glusterd2.log"]
    if g.config.get("gluster")['client_gluster_logs_info']['dirs']:
        client_gluster_logs_dirs = (
                g.config['gluster']['client_gluster_logs_info']['dirs'])

    if g.config.get("gluster")['client_gluster_logs_info']['files']:
        client_gluster_logs_files = (
                g.config['gluster']['client_gluster_logs_info']['files'])

    # Have a unique string to recognize the test run for logging in
    # gluster logs
    if 'glustotest_run_id' not in g.config:
        g.config['glustotest_run_id'] = (
            datetime.datetime.now().strftime('%H_%M_%d_%m_%Y'))
    glustotest_run_id = g.config['glustotest_run_id']
    g.log.info("Glusto test run id %s", glustotest_run_id)
    return (server_gluster_logs_dirs, server_gluster_logs_files,
            client_gluster_logs_dirs, client_gluster_logs_files,
            glustotest_run_id)
