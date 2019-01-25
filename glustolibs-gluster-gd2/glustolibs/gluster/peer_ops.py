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

"""
     Description: Library for gluster peer operations.
"""

import json
import httplib
from glustolibs.gluster.rest import RestClient
from glusto.core import Glusto as g

def peer_probe(mnode, server):
    """Probe the specified server.

    Args:
        mnode (str): Node on which command has to be executed.
        server (str): Server to be peer probed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the error message
            and error code of the the command execution.
    """

    data = {"addresses": [server]}
    return RestClient(mnode).handle_request('POST', "/v1/peers", httplib.CREATED, data)


def pool_list(mnode):
    """Runs 'gluster pool list' command on the specified node.

    Args:
        mnode (str): Node on which command has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """
    return RestClient(mnode).handle_request('GET', "/v1/peers", httplib.OK, None)


def peer_detach(mnode, server):
    """ Detach the specified server.

    Args:
        mnode (str): Node on which command has to be executed.
        server (str): Server to be peer detached.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
    """

    server_id = get_peer_id(mnode, server)
    ret, out, err = RestClient(mnode).handle_request('DELETE', "/v1/peers/%s"
                                                     % server_id, httplib.NO_CONTENT, None)
    if ret != httplib.NO_CONTENT:
        returncode = 1
        g.log.error("Failed to peer detach the node '%s'.", server)
    else:
        returncode = 0

    return (returncode, out, err)


def peer_status(mnode, peer=None):
    """ Fetches the peer status

    Args:
        mnode (str): Node on which command has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
     """

    path = "/v1/peers"
    if peer:
        peerid = get_peer_id(mnode, peer)
        path = "%s/%s" % (path, peerid)
    return RestClient(mnode).handle_request('GET', path, httplib.OK, None)


def peer_edit(mnode, peerid, zone):
    """ Edits the peer zone
_
    Args:
        mnode (str): Node on which command has to be executed.
        peerid (str): The peerid of the peer.
        Zone (str): The zone details that has to be edited.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.
     """

    data = {"metadata": {"zone": zone}}
    return RestClient(mnode).handle_request("POST", "/v1/peers/%s" % peerid,
                                            httplib.CREATED, data)


def get_peer_id(mnode, server):
    """
    Returns the peer_id of the given server

    Args:
        server (str) : A server to fetch peer-ids

    Returns:
        server_id (str) : Peer-id of the given server/peer
    """
    from glustolibs.gluster.lib_utils import get_ip_from_hostname

    _ip = node = ids = []
    _ip = get_ip_from_hostname([server])
    server = ''.join(_ip)
    _, out, _ = pool_list(mnode)
    output = json.loads(out)
    for elem in output:
        item = elem['client-addresses'][1].split(":")
        node.append(item[0])
        item = elem['id']
        ids.append(item)
        if server in node:
            return ids[-1]

def is_peer_connected(mnode, servers):
    """Checks whether specified peer is in cluster and 'Connected' state.

    Args:
        mnode (str): Node from which peer probe has to be executed.
        servers (str): A server| list of servers to be validated.

    Returns
        bool : True on success (peer in cluster and connected), False on
            failure.
    """
    from glustolibs.gluster.lib_utils import to_list

    servers = to_list(servers)

    for server in servers:
        _, out, _ = peer_status(mnode, server)
        out = json.loads(out)
        if not out['online']:
            g.log.error("The peer %s is not connected", server)
            return False
    return True


def nodes_from_pool_list(mnode):
    """Return list of nodes from the 'gluster pool list'.

    Args:
        mnode (str): Node on which command has to be executed.

    Returns:
        NoneType: None if command execution fails.
        list: List of nodes in pool on Success, Empty list on failure.
    """
    _, pool_list_data, _ = pool_list(mnode)
    server_list = json.loads(pool_list_data)
    if server_list is None:
        g.log.error("Unable to get Nodes from the pool list command.")
        return None

    nodes = []
    for server in server_list:
        nodes.append(server['name'])
    return nodes


def peer_probe_servers(mnode, servers, validate=True):
    """Probe specified servers and validate whether probed servers
    are in cluster and connected state if validate is set to True.

    Args:
        mnode (str): Node on which command has to be executed.
        servers (str|list): A server|List of servers to be peer probed.

    Kwargs:
        validate (bool): True to validate if probed peer is in cluster and
            connected state. False otherwise. Defaults to True.

    Returns:
        bool: True on success and False on failure.
    """
    from glustolibs.gluster.lib_utils import to_list

    servers = to_list(servers)

    if mnode in servers:
        servers.remove(mnode)

    # Get list of nodes from 'gluster pool list'
    nodes_in_pool_list = nodes_from_pool_list(mnode)
    if not nodes_in_pool_list:
        g.log.error("Unable to get nodes from gluster pool list. "
                    "Failing peer probe.")
        return False

    for server in servers:
        if server not in nodes_in_pool_list:
            ret, _, _ = peer_probe(mnode, server)
            if ret != 0:
                g.log.error("Failed to peer probe the node '%s'.", server)
                return False
            g.log.info("Successfully peer probed the node '%s'.", server)

    # Validating whether peer is in connected state after peer probe
    if validate:
        _rc = False
        i = 0
        while i < 200:
            if is_peer_connected(mnode, servers):
                _rc = True
                break

        if not _rc:
            g.log.error("Peers are in not connected state")
        g.log.info("All peers are in connected state")
        return _rc


def peer_detach_servers(mnode, servers, validate=True):
    """Detach peers and validate status of peer if validate is set to True.

    Args:
        mnode (str): Node on which command has to be executed.
        servers (str|list): A server|List of servers to be detached.

    Kwargs:
        validate (bool): True if status of the peer needs to be validated,
            False otherwise. Defaults to True.

    Returns:
        bool: True on success and False on failure.
    """

    from glustolibs.gluster.lib_utils import to_list

    servers = to_list(servers)

    if mnode in servers:
        servers.remove(mnode)

    for server in servers:
        ret, _, _ = peer_detach(mnode, server)
        if ret:
            g.log.error("Failed to peer detach the node '%s'.", server)
            return False

    # Validating whether peer detach is successful
    if validate:
        i = 0
        while i < 200:
            count = 0
            nodes_in_pool = nodes_from_pool_list(mnode)
            _rc = True
            for server in servers:
                if server in nodes_in_pool:
                    g.log.error("Peer '%s' still in pool", server)
                    _rc = False
                    count += 1
            if not count:
                break

        if not _rc:
            g.log.error("Validation after peer detach failed.")
        g.log.info("Validation after peer detach is successful")
        return _rc
