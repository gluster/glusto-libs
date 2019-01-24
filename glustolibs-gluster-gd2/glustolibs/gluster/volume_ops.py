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

import json
import httplib
from glusto.core import Glusto as g
from glustolibs.gluster.rest import RestClient
from glustolibs.gluster.lib_utils import validate_uuid
from glustolibs.gluster.exceptions import GlusterApiInvalidInputs


"""This module contains the python glusterd2 volume api's implementation."""


def validate_brick(bricks_list):
    """Validate brick pattern.
    Args:
        bricks(list): in the form of ["nodeid:brickpath"]
    Returns:
        brick_req(list): list of bricks
    """
    brick_req = []
    result = True
    if bricks_list:
        for brick in bricks_list:
            brk = brick.split(":")
            if len(brk) != 2 or not validate_uuid(brk[0]):
                result = None
                break
            req = {}
            req['peerid'] = brk[0]
            req['path'] = brk[1]
            brick_req.append(req)
    else:
        result = None

    if result:
      return brick_req
    else:
      return result


def volume_create(mnode, volname, bricks_list, force=False, replica_count=0,
                  arbiter_count=0, transport_type="tcp",
                  options=None, metadata=None):
    """Create the gluster volume with specified configuration
    Args:
        mnode(str): server on which command has to be executed
        volname(str): volume name that has to be created
        bricks_list (list): List of bricks to use for creating volume.
            Example:
                from glustolibs.gluster.lib_utils import form_bricks_list
                bricks_list = form_bricks_list(mnode, volname, num_of_bricks,
                                               servers, servers_info)
        Kwargs:
            force (bool): If this option is set to True, then create volume
                will get executed with force option. If it is set to False,
                then create volume will get executed without force option
            replica_count (int): if volume is replicated type
            arbiter_count (int):if volume is arbiter type
            transport_type : tcp, rdma
            options (dict): volume options
            metadata (dict): volume metadata
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
           (-1, '', ''): If not enough bricks are available to create volume.
           (ret, out, err): As returned by volume create command execution.
    Example:
        volume_create(mnode, volname, bricks_list)
    """

    if len(bricks_list) <= 0:
            raise GlusterApiInvalidInputs("Bricks cannot be empty")

    req_bricks = validate_brick(bricks_list)
    if not req_bricks:
        raise GlusterApiInvalidInputs("Invalid Brick details, bricks "
                                      "should be in form of "
                                      "<peerid>:<path>")

    if transport_type not in ("tcp", "rdma", "tcp,rdma"):
            raise GlusterApiInvalidInputs("Transport type %s not "
                                          "supported" % transport_type)

    if not options:
        options = {}

    if not metadata:
        metadata = {}

    num_bricks = len(bricks_list)
    sub_volume = []

    if replica_count > 0:
        replica = arbiter_count + replica_count

        if num_bricks % replica != 0:
            raise GlusterApiInvalidInputs(
                    "Invalid number of bricks specified")

        num_subvol = num_bricks / replica
        for i in range(0, num_subvol):
            idx = i * replica
            ida = i * replica + 2
            # If Arbiter is set, set it as Brick Type for 3rd th brick
            if arbiter_count > 0:
                req_bricks[ida]['type'] = 'arbiter'
            subvol_req = {}
            subvol_req['type'] = 'replicate'
            subvol_req['bricks'] = req_bricks[idx:idx + replica]
            subvol_req['replica'] = replica_count
            subvol_req['arbiter'] = arbiter_count
            sub_volume.append(subvol_req)
    else:
        subvol_req = {}
        subvol_req['type'] = 'distrubute'
        subvol_req['bricks'] = req_bricks
        sub_volume.append(subvol_req)

    # To create a brick dir
    create_brick_dir = {"create-brick-dir": True}

    data = {
            "name": volname,
            "subvols": sub_volume,
            "transport": transport_type,
            "options": options,
            "force": force,
            "metadata": metadata,
            "Flags": create_brick_dir
            }

    return RestClient(mnode).handle_request(
            "POST", "/v1/volumes", httplib.CREATED, data)


def volume_start(mnode, volname, force=False):
    """Starts the gluster volume
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Kwargs:
        force (bool): If this option is set to True, then start volume
            will get executed with force option. If it is set to False,
            then start volume will get executed without force option
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_start("w.x.y.z", "testvol")
    """
    data = {
            "force-start-bricks": force
           }
    return RestClient(mnode).handle_request(
            "POST", "/v1/volumes/%s/start" % volname,
            httplib.OK, data)


def volume_stop(mnode, volname, force=False):
    """Stops the gluster volume
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Kwargs:
        force (bool): If this option is set to True, then stop volume
            will get executed with force option. If it is set to False,
            then stop volume will get executed without force option
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_stop(w.x.y.z, "testvol")
    """
    return RestClient(mnode).handle_request(
            "POST", "/v1/volumes/%s/stop" % volname,
            httplib.OK, None)


def volume_delete(mnode, volname, xfail=False):
    """Deletes the gluster volume if given volume exists in
       gluster and deletes the directories in the bricks
       associated with the given volume
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Kwargs:
        xfail (bool): expect to fail (non existent volume, etc.)
    Returns:
        bool: True, if volume is deleted
              False, otherwise
    Example:
        volume_delete("w.x.y.z", "testvol")
    """
    hosts = []
    paths = []
    volinfo = get_volume_info(mnode, volname, xfail)
    if not volinfo:
        if xfail:
            g.log.info(
                "Volume {} does not exist in {}"
                .format(volname, mnode)
            )
            return True
        else:
            g.log.error(
                "Unexpected: volume {} does not exist in {}"
                .format(volname, mnode))
            return False

    _, _, err = RestClient(mnode).handle_request(
            "DELETE", "/v1/volumes/%s" % volname,
            httplib.NO_CONTENT, None)
    if err:
        if xfail:
            g.log.info("Volume delete is expected to fail")
            return True

        g.log.error("Volume delete failed")
        return False

    # remove all brick directories
    for j in volinfo['subvols']:
        for i in j['bricks']:
            g.run(i['host'], "rm -rf %s" % i['path'])

    return True


def volume_reset(mnode, volname, force=False,
                 options=None, all_volumes=False):
    """Resets the gluster volume
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Kwargs:
        force (bool): If this option is set to True, then reset volume
            will get executed with force option. If it is set to False,
            then reset volume will get executed without force option.
        options (dict): volume options
        all_volumes (bool)
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_reset("w.x.y.z", "testvol")`
    """
    if not 'options':
        options = {}
    data = {
            "options": options,
            "force": force,
            "all": all_volumes,
            }
    return RestClient(mnode).handle_request(
            "DELETE", "/v1/volumes/%s/options" % volname,
            httplib.OK, data)


def volume_info(mnode, volname):
    """Get gluster volume info
    Args:
        mnode (str): Node on which cmd has to be executed.
    Kwargs:
        volname (str): volume name.
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_info("w.x.y.z")
    """
    return RestClient(mnode).handle_request("GET",
                                            "/v1/volumes/%s" % volname,
                                            httplib.OK, None)


def get_volume_info(mnode, volname, xfail=False):
    """Fetches the volume information as displayed in the volume info.
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name.
    Kwargs:
        xfail (bool): Expect failure to get volume info
    Returns:
        NoneType: If there are errors
        dict: volume info in dict of dicts
    Example:
        get_volume_info("abc.com", volname="testvol")
    """
    ret, vol_info, err = volume_info(mnode, volname)
    if ret:
        if xfail:
            g.log.error(
                    "Unexpected: volume info {} returned err ({} : {})"
                    .format(volname, vol_info, err)
                    )
        return None
    vol_info = json.loads(vol_info)
    g.log.info("Volume info: %s", vol_info)
    return vol_info


def volume_status(mnode, volname):
    """Get gluster volume status
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name.
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_status("w.x.y.z", "testvol")
    """
    return RestClient(mnode).handle_request(
            "GET", "/v1/volumes/%s/status" % volname,
            httplib.OK, None)


def get_volume_status(mnode, volname, service=''):
    """This module gets the status of all or specified volume(s)/brick
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name.
    Kwargs:
        service (str): name of the service to get status
            can be bricks
    Returns:
        dict: volume status in dict of dictionary format, on success
        NoneType: on failure
    Example:
        get_volume_status("10.70.47.89", volname="testvol")
    """
    if service:
        _, status, err = volume_brick_status(mnode, volname)
    else:
        _, status, err = volume_status(mnode, volname)
    if not err:
        status = json.loads(status)
        return status
    return None


def volume_brick_status(mnode, volname):
    """Get gluster volume brick status
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_status("w.x.y.z","testvol")
    """
    return RestClient(mnode).handle_request(
            "GET", "/v1/volumes/%s/bricks" % volname,
            httplib.OK, None)


def volume_list(mnode):
    """List the gluster volume
    Args:
        mnode (str): Node on which cmd has to be executed.
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            The second element 'out' is of type 'str' and is the output of
            the operation
            The third element 'err|status' code on failure.
            Otherwise None.
    Example:
        volume_list("w.x.y.z")
    """
    return RestClient(mnode).handle_request(
            "GET", "/v1/volumes", httplib.OK, None)


def get_volume_list(mnode, xfail=False):
    """Fetches the volume names in the gluster.
    Args:
        mnode (str): Node on which cmd has to be executed.
    Kwargs:
        xfail (bool): Expect failure to get volume info
    Returns:
        NoneType: If there are errors
        list: List of volume names
    Example:
        get_volume_list("w.x.y.z")
    """
    vol_list = []
    ret, volumelist, err = volume_list(mnode)
    if ret:
        if xfail:
            g.log.error(
                    "Unexpected: volume list returned err ({} : {})"
                    .format(volumelist, err)
                    )
        return None
    volumelist = json.loads(volumelist)
    for i in volumelist:
        vol_list.append(i["name"])
    g.log.info("Volume list: %s", vol_list)
    return vol_list


def get_volume_options(mnode, volname, option=None):
    """Gets the option values for the given volume.
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
    Kwargs:
        option (str): volume option to get status.
                    If not given, the function returns all the options for
                    the given volume
    Returns:
        dict: value for the given volume option in dict format, on success
        NoneType: on failure
    Example:
        get_volume_options(mnode, "testvol")
    """
    if not option:
        _, get_vol_options, err = RestClient(mnode).handle_request(
            "GET", "/v1/volumes/%s/options" % volname, httplib.OK, None)
    else:
        _, get_vol_options, err = RestClient(mnode).handle_request(
            "GET", "/v1/volumes/%s/options/%s" % (volname, option),
            httplib.OK, None)
    if not err:
        get_vol_options = json.loads(get_vol_options)
        return get_vol_options
    return None


def set_volume_options(mnode, volname, options,
                       advance=True, experimental=False,
                       deprecated=False):
    """Sets the option values for the given volume.
    Args:
        mnode (str): Node on which cmd has to be executed.
        volname (str): volume name
        options (dict): volume options in key
            value format
    Kwargs:
        advance (bool): advance flag to set options. Default set True
        experimental (bool): experimental flag to set options.
                             Default set False.
        deprecated (bool): deprecated flag to set options.
                           Default set False
    Returns:
        bool: True, if the volume option is set
              False, on failure
    Example:
        set_volume_option("w.x.y.z", "testvol", options)
    """
    if not options:
        raise GlusterApiInvalidInputs("cannot set empty options")

    vol_options = {}
    req = {}
    for key in options:
        vol_options[key] = options[key]
    req['options'] = vol_options
    req['allow-advanced-options'] = advance
    req['allow-experimental-options'] = experimental
    req['allow-deprecated-options'] = deprecated
    _, _, err = RestClient(mnode).handle_request(
        "POST", "/v1/volumes/%s/options" % volname,
        httplib.CREATED, req)
    if err:
        return True
    return False
