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
    Description: Library for gluster device operation.
"""


import httplib
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import GlusterApiInvalidInputs
from glustolibs.gluster.lib_utils import validate_peer_id
from glustolibs.gluster.rest import RestClient


def rest_call(ops, mnode, method, path, code, data):
    """
    To handle the get methods of devices
    Args:
        ops (str): operation performing on devices
        mnode (str): Node on which commands as to run
        method (str): rest methods, i.e POST, GET, DELETE
        path(str): path of the operation
                   e.g: /v1/devices
        code(str): status code of the operation
        data(dict): json input
    Returns:
        tuple: Tuple containing two elements (ret, out|err)
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'dict' and is the
        error message and code of operation on failure
    """
    output = ""
    ret, out, err = RestClient(mnode).handle_request(method, path, code, data)
    if ret:
        g.log.error("Failed to perform device %s operation", ops)
        output = err
    else:
        output = out
    return ret, output


def device_add(mnode, peerid, device):
    """
    Gluster device add.
    Args:
        mnode (string) : Node on which command as to run
        peerid (string) : Peer UUID
        device (string) : device name
    Returns:
    tuple: Tuple containing two elements (ret, out|err).
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'str' and is the
        error message and code of operation on failure
    """
    validate_peer_id(peerid)
    if not device:
        raise GlusterApiInvalidInputs("Invalid device specified %s" % device)
    data = {
        "Device": device
        }
    return rest_call("add", mnode, "POST",
                     "/v1/devices/%s" % peerid,
                     httplib.CREATED, data)


def device_info(mnode, peerid, device):
    """
    Gluster get devices in peer.
    Args:
        mnode (string) : Node on which command as to run
        peerid (string): peerid returned from peer_add
        devices (dict): device which info needed
    Returns:
    tuple: Tuple containing two elements (ret, out|err).
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'str' and is the
        error message and code of operation on failure
    """
    validate_peer_id(peerid)
    if not device:
        raise GlusterApiInvalidInputs("Invalid device specified %s" % device)
    device = {"device": device}
    return rest_call("info", mnode, "GET",
                     "/v1/devices/%s/%s" % (peerid, device),
                     httplib.OK, None)


def devices_in_peer(mnode, peerid):
    """
    Gluster get devices in peer.
    Args:
        mnode (string) : Node on which command as to run
        peerid (string): peerid returned from peer_add
    Returns:
    tuple: Tuple containing two elements (ret, out|err).
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'str' and is the
        error message and code of operation on failure
    """
    validate_peer_id(peerid)
    return rest_call("list", mnode, "GET",
                     "/v1/devices/%s" % peerid,
                     httplib.OK, None)


def devices(mnode):
    """
    Gluster list all devices.
    Args:
        mnode (string) : Node on which command as to run
    Returns:
    tuple: Tuple containing three elements (ret, out|err).
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'str' and is the
        error message and code of operation on failure.
    """
    return rest_call("list", mnode, "GET",
                     "/v1/devices", httplib.OK, None)


def device_edit(mnode, peerid, device, state):
    """
    Gluster edit the device
    Args:
        mnode (string) : Node on which command as to run
        device (string) : device name
        state (string) : state of the device.
                         Either enabled or disabled
    Returns:
    tuple: Tuple containing two elements (ret, out|err).
        The first element 'ret' is of type 'int' and is the return value
        The second element 'out' is of type 'str' and is the output of
        the operation on success
        The third element 'err' is of type 'str' and is the
        error message and code of operation on failure
    """
    validate_peer_id(peerid)
    if not device:
        raise GlusterApiInvalidInputs("Invalid device specified %s" % device)
    device = {"device": device}
    data = {
        "state": state
        }
    return rest_call("edit", mnode, "POST",
                     "/v1/devices/%s/%s" % (peerid, device),
                     httplib.CREATED, data)
