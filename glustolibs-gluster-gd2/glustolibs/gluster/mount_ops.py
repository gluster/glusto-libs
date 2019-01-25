#!/usr/bin/env python
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
    Description: Module for Mount operations.
"""


from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ConfigError
import copy

class GlusterMount():
    """Gluster Mount class
    Args:
        mount (dict): Mount dict with mount_protocol, mountpoint,
            server, client, volname, options
        Example:
            mount =
                {'protocol': 'glusterfs',
                 'mountpoint': '/mnt/g1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': 'def.lab.eng.xyz.com',
                 'volname': 'testvol',
                 'options': ''
                 }
    Returns:
        Instance of GlusterMount class
   """
    def __init__(self, mount):
        # Check for missing parameters
        for param in ['protocol', 'mountpoint', 'server',
                      'client', 'volname', 'options']:
            if param not in mount:
                raise ConfigError("Missing key %s" % param)

        # Get Protocol
        self.mounttype = mount.get('protocol', 'glusterfs')

        # Get mountpoint
        if bool(mount.get('mountpoint', False)):
            self.mountpoint = mount['mountpoint']
        else:
            self.mountpoint = "/mnt/%s" % self.mounttype

        # Get server
        self.server_system = mount.get('server', None)

        # Get client
        self.client_system = mount.get('client', None)

        # Get Volume name
        self.volname = mount['volname']

        # Get options
        self.options = mount.get('options', None)

    def mount(self):
        """Mounts the volume
        Args:
            uses instance args passed at init
        Returns:
            bool: True on success and False on failure.
        """
        ret, out, err = mount_volume(self.volname, mtype=self.mounttype,
                                     mpoint=self.mountpoint,
                                     mserver=self.server_system,
                                     mclient=self.client_system,
                                     options=self.options)
        if ret:
            g.log.error("Failed to mount the volume")
            return False
        return True

    def is_mounted(self):
        """Tests for mount on client
        Args:
            uses instance args passed at init
        Returns:
            bool: True on success and False on failure.
        """
        ret = is_volume_mounted(self.volname,
                                mpoint=self.mountpoint,
                                mserver=self.server_system,
                                mclient=self.client_system,
                                mtype=self.mounttype)

        if not ret:
            g.log.error("Volume is not mounted")
            return False
        return True

    def unmount(self):
        """Unmounts the volume
        Args:
            uses instance args passed at init
        Returns:
            bool: True on success and False on failure.
        """
        (ret, out, err) = umount_volume(mclient=self.client_system,
                                        mpoint=self.mountpoint,
                                        mtype=self.mounttype)
        if ret:
            g.log.error("Failed to unmount the volume")
            return False
        return True


def is_volume_mounted(volname, mpoint, mserver, mclient, mtype):
    """Check if mount exist.
    Args:
        volname (str): Name of the volume
        mpoint (str): Mountpoint dir
        mserver (str): Server to which it is mounted to
        mclient (str): Client from which it is mounted.
        mtype (str): Mount type (glusterfs)
    Returns:
        bool: True if mounted and False otherwise.
    """
    # python will error on missing arg, so just checking for empty args here
    for param in [volname, mpoint, mserver, mclient, mtype]:
        if not param:
            g.log.error("Missing arguments for mount.")
            return False

    ret, _, _ = g.run(mclient, "mount | grep %s | grep %s | grep \"%s\""
                      % (volname, mpoint, mserver))
    if ret:
        g.log.debug("Volume %s is mounted at %s:%s" % (volname, mclient,
                                                       mpoint))
        return True
    g.log.error("Volume %s is not mounted at %s:%s" % (volname,
                                                       mclient,
                                                       mpoint))
    return False


def mount_volume(volname, mtype, mpoint, mserver, mclient, options=''):
    """Mount the gluster volume with specified options.
    Args:
        volname (str): Name of the volume to mount.
        mtype (str): Protocol to be used to mount.
        mpoint (str): Mountpoint dir.
        mserver (str): Server to mount.
        mclient (str): Client from which it has to be mounted.
    Kwargs:
        option (str): Options for the mount command.
    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            (0, '', '') if already mounted.
            (ret, out, err) of mount commnd execution otherwise.
    """
    if is_mounted(volname, mpoint, mserver, mclient, mtype):
        g.log.debug("Volume %s is already mounted at %s" %
                    (volname, mpoint))
        return (0, '', '')

    if not options:
        options = "-o %s" % options

    # Create mount dir
    g.run(mclient, "test -d %s || mkdir -p %s" % (mpoint, mpoint))

    # Mount
    mcmd = ("mount -t %s %s %s:/%s %s" %
            (mtype, options, mserver, volname, mpoint))

    # Create mount
    return g.run(mclient, mcmd)


def umount_volume(mclient, mpoint, mtype=''):
    """Unmounts the mountpoint.
    Args:
        mclient (str): Client from which it has to be mounted.
        mpoint (str): Mountpoint dir.
    Kwargs:
        mtype (str): Mounttype. Defaults to ''.
    Returns:
        tuple: Tuple containing three elements (ret, out, err) as returned by
            umount command execution.
    """
    cmd = ("umount %s || umount -f %s || umount -l %s" %
            (mpoint, mpoint, mpoint))

    return g.run(mclient, cmd)


def create_mount_objs(mounts):
    """Creates GlusterMount class objects for the given list of mounts
    Args:
        mounts (list): list of mounts with each element being dict having the
            specifics of each mount
        Example:
            mounts: [
                {'protocol': 'glusterfs',
                 'mountpoint': '/mnt/g1',
                 'server': 'abc.lab.eng.xyz.com',
                 'client': {'host': 'def.lab.eng.xyz.com'},
                 'volname': 'testvol',
                 'options': '',
                 'num_of_mounts': 2}
                ]
    Returns:
        list : List of GlusterMount class objects.
    Example:
        mount_objs = create_mount_objs(mounts)
    """
    mount_obj_list = []
    for mount in mounts:
        temp_mount = copy.deepcopy(mount)
        if (mount['protocol'] == "glusterfs"):
            if 'mountpoint' in mount and mount['mountpoint']:
                temp_mount['mountpoint'] = mount['mountpoint']
            else:
                temp_mount['mountpoint'] = ("/mnt/%s_%s" %
                                            (mount['volname'],
                                             mount['protocol']))

        num_of_mounts = 1
        if 'num_of_mounts' in mount:
            if mount['num_of_mounts']:
                num_of_mounts = mount['num_of_mounts']
        if num_of_mounts > 1:
            mount_dir = temp_mount['mountpoint']
            for count in range(1, num_of_mounts + 1):
                if mount_dir != "*":
                    temp_mount['mountpoint'] = '_'.join(
                        [mount_dir, str(count)])

                mount_obj_list.append(GlusterMount(temp_mount))
        else:
            mount_obj_list.append(GlusterMount(temp_mount))

    return mount_obj_list


def operate_mounts(mount_objs, operation):
    """Mounts/Unmounts using the details as specified
    in the each mount obj
    Args:
        mount_objs (list): list of mounts objects with each element being
            the GlusterMount class object
        operation (str): Mount/unmount
    Returns:
        bool : True if creating the mount for all mount_objs is successful.
            False otherwise.
    Example:
        ret = operate_mounts(create_mount_objs(mounts), operation='mount')
    """
    _rc = True
    for mount_obj in mount_objs:
        if operation == 'mount':
            ret = mount_obj.mount()
        elif operation == 'unmount':
            ret = mount_obj.unmount()
        else:
            g.log.error("Operation not found")
            _rc = False
        return _rc


def create_mounts(mount_objs):
    """Creates Mounts using the details as specified in the each mount obj
    Args:
        mount_objs (list): list of mounts objects with each element being
            the GlusterMount class object
    Returns:
        bool : True if creating the mount for all mount_objs is successful.
            False otherwise.
    Example:
        ret = create_mounts(create_mount_objs(mounts))
    """
    return operate_mounts(mount_objs, operation='mount')


def unmount_mounts(mount_objs):
    """Unmounts using the details as specified in the each mount obj
    Args:
        mount_objs (list): list of mounts objects with each element being
            the GlusterMount class object
    Returns:
        bool : True if unmounting the mount for all mount_objs is successful.
            False otherwise.
    Example:
        ret = unmount_mounts(create_mount_objs(mounts))
    """
    return operate_mounts(mount_objs, operation='unmount')
