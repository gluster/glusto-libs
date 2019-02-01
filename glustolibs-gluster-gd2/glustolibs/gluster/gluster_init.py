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
    Description: This file contains the methods for starting/stopping glusterd2
        and other initial gluster environment setup helpers.
"""

from glusto.core import Glusto as g
from glustolibs.gluster.lib_utils import to_list


def operate_glusterd(servers, operation):
    """
    Performs start/stop/restart glusterd2 on specified
    servers according to mentioned operation.
    Args:
        servers (str|list): A server|List of server hosts on which glusterd2
            has to be started.
        operation (str) : start/stop/restart glusterd2
    Returns:
        bool : True if operation performed on glusterd2 is successful
            on all servers.False otherwise.
    """
    servers = to_list(servers)

    if operation == "start":
        cmd = "pgrep glusterd2 || systemctl start glusterd2"

    if operation == "stop":
        cmd = "systemctl stop glusterd2"

    if operation == "restart":
        cmd = "systemctl restart glusterd2"

    results = g.run_parallel(servers, cmd)

    _rc = True
    for server, ret_values in results.iteritems():
        retcode, _, _ = ret_values
        if retcode:
            g.log.error("Unable to %s glusterd2 on server "
                        "%s", operation, server)
            _rc = False

    return _rc


def start_glusterd(servers):
    """Starts glusterd2 on specified servers if they are not running.
    Args:
        servers (str|list): A server|List of server hosts on which glusterd2
            has to be started.
    Returns:
        bool : True if starting glusterd2 is successful on all servers.
            False otherwise.
    """
    return operate_glusterd(servers, "start")


def stop_glusterd(servers):
    """Stops the glusterd2 on specified servers.
    Args:
        servers (str|list): A server|List of server hosts on which glusterd2
            has to be stopped.
    Returns:
        bool : True if stopping glusterd2 is successful on all servers.
            False otherwise.
    """
    return operate_glusterd(servers, "stop")


def restart_glusterd(servers):
    """Restart the glusterd2 on specified servers.
    Args:
        servers (str|list): A server|List of server hosts on which glusterd2
            has to be restarted.
    Returns:
        bool : True if restarting glusterd2 is successful on all servers.
            False otherwise.
    """
    return operate_glusterd(servers, "restart")


def is_glusterd_running(servers):
    """Checks the glusterd status on specified servers.
    Args:
        servers (str|list): A server|List of server hosts on which glusterd
            status has to be checked.
    Returns:
            0  : if glusterd running
            1  : if glusterd not running
           -1  : if glusterd not running and PID is alive
    """
    servers = to_list(servers)

    cmd1 = "systemctl status glusterd2"
    cmd2 = "pidof glusterd2"
    cmd1_results = g.run_parallel(servers, cmd1)
    cmd2_results = g.run_parallel(servers, cmd2)

    _rc = 0
    for server, ret_values in cmd1_results.iteritems():
        retcode, _, _ = ret_values
        if retcode:
            g.log.error("glusterd2 is not running on the server %s", server)
            _rc = 1
            if not cmd2_results[server][0]:
                g.log.error("PID of glusterd2 is alive and status is not "
                            "running")
                _rc = -1
    return _rc


def get_glusterd_pids(nodes):
    """
    Checks if glusterd process is running and
    return the process id's in dictionary format
    Args:
        nodes ( str|list ) : Node/Nodes of the cluster
    Returns:
        tuple : Tuple containing two elements (ret, gluster_pids).
        The first element 'ret' is of type 'bool', True if only if
        glusterd is running on all the nodes in the list and each
        node contains only one instance of glusterd running.
        False otherwise.
        The second element 'glusterd_pids' is of type dictonary and
        it contains the process ID's for glusterd.
    """
    glusterd_pids = {}
    _rc = True
    nodes = to_list(nodes)

    cmd = "pidof glusterd2"
    g.log.info("Executing cmd: %s on node %s", cmd, nodes)
    results = g.run_parallel(nodes, cmd)
    for node in results:
        ret, out, _ = results[node]
        output = out.strip()
        splited_output = output.split("\n")
        if not ret:
            if len(splited_output):
                if not output:
                    g.log.error("NO glusterd2 process found or "
                                "gd2 is not running on the node %s", node)
                    _rc = False
                    glusterd_pids[node] = ['-1']
                else:
                    g.log.info("glusterd2 process with "
                               "pid %s found on %s",
                               splited_output, node)
                    glusterd_pids[node] = (splited_output)
            else:
                g.log.error("More than one glusterd2 process "
                            "found on node %s", node)
                _rc = False
                glusterd_pids[node] = out
        else:
            g.log.error("Not able to get glusterd2 process "
                        "or glusterd2 process is"
                        "killed on node %s", node)
            _rc = False
            glusterd_pids[node] = ['-1']
    return _rc, glusterd_pids
