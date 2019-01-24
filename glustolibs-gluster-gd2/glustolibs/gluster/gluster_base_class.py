#  Copyright (C) 2019 Red Hat, Inc. <http://www.redhat.com>
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
#
"""
    Description: Module containing GlusterBaseClass which defines all the
        variables necessary for tests.
"""

import unittest
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ConfigError, ExecutionError
from glustolibs.gluster.peer_ops import is_peer_connected, peer_status
from glustolibs.io.utils import log_mounts_info
from glustolibs.gluster.lib_utils import (
    get_ip_from_hostname, configure_volumes,
    configure_mounts, inject_msg_in_gluster_logs,
    configure_logs, set_conf_entity)
from glustolibs.gluster.volume_ops import set_volume_options
from glustolibs.gluster.volume_libs import (setup_volume,
                                            cleanup_volume,
                                            log_volume_info_and_status)


class runs_on(g.CarteTestClass):
    """Decorator providing runs_on capability for standard unittest script"""

    def __init__(self, value):
        # the names of the class attributes set by the runs_on decorator
        self.axis_names = ['volume_type', 'mount_type']

        # the options to replace 'ALL' in selections
        self.available_options = [['distributed', 'replicated',
                                   'distributed-replicated'],
                                  ['glusterfs']]

        # these are the volume and mount options to run and set in config
        # what do runs_on_volumes and runs_on_mounts need to be named????
        run_on_volumes = self.available_options[0]
        run_on_mounts = self.available_options[1]
        if g.config.get('gluster', "")['running_on_volumes']:
            run_on_volumes = g.config['gluster']['running_on_volumes']
        if g.config.get('gluster', "")['running_on_mounts']:
            run_on_mounts = g.config['gluster']['running_on_mounts']

        # selections is the above info from the run that is intersected with
        # the limits from the test script
        self.selections = [run_on_volumes, run_on_mounts]

        # value is the limits that are passed in by the decorator
        self.limits = value


class GlusterBaseClass(unittest.TestCase):
    """GlusterBaseClass to be subclassed by Gluster Tests.
    This class reads the config for variable values that will be used in
    gluster tests. If variable values are not specified in the config file,
    the variable are defaulted to specific values.
    """

    volume_type = None
    mount_type = None

    @classmethod
    def validate_peers_are_connected(cls):
        """Validate whether each server in the cluster is connected to
        all other servers in cluster.
        Returns (bool): True if all peers are in connected with other peers.
            False otherwise.
        """
        # Validate if peer is connected from all the servers
        g.log.info("Validating if servers %s are connected from other servers "
                   "in the cluster", cls.servers)
        for server in cls.servers:
            g.log.info("Validate servers %s are in connected from  node %s",
                       cls.servers, server)
            ret = is_peer_connected(server, cls.servers)
            if not ret:
                g.log.error("Some or all servers %s are not in connected "
                            "state from node %s", cls.servers, server)
                return False
            g.log.info("Successfully validated servers %s are all in "
                       "connected state from node %s",
                       cls.servers, server)
        g.log.info("Successfully validated all servers %s are in connected "
                   "state from other servers in the cluster", cls.servers)

        # Peer Status from mnode
        peer_status(cls.mnode)

        return True

    @classmethod
    def setup_volume(cls, volume_create_force=False):
        """Setup the volume:
            - Create the volume, Start volume, Set volume
            options, enable snapshot/quota/tier if specified in the config
            file.
            - Wait for volume processes to be online
            - Export volume as NFS/SMB share if mount_type is NFS or SMB
            - Log volume info and status
        Args:
            volume_create_force(bool): True if create_volume should be
                executed with 'force' option.
        Returns (bool): True if all the steps mentioned in the descriptions
            passes. False otherwise.
        """
        force_volume_create = False
        if cls.volume_create_force:
            force_volume_create = True

        # Validate peers before setting up volume
        g.log.info("Validate peers before setting up volume ")
        ret = cls.validate_peers_are_connected()
        if not ret:
            g.log.error("Failed to validate peers are in connected state "
                        "before setting up volume")
            return False
        g.log.info("Successfully validated peers are in connected state "
                   "before setting up volume")

        # Setup Volume
        g.log.info("Setting up volume %s", cls.volname)
        ret = setup_volume(mnode=cls.mnode,
                           all_servers_info=cls.all_servers_info,
                           volume_config=cls.volume, force=force_volume_create)
        if not ret:
            g.log.error("Failed to Setup volume %s", cls.volname)
            return False
        g.log.info("Successful in setting up volume %s", cls.volname)

        # ToDo : Wait for volume processes to be online

        # Log Volume Info and Status
        g.log.info("Log Volume %s Info and Status", cls.volname)
        ret = log_volume_info_and_status(cls.mnode, cls.volname)
        if not ret:
            g.log.error("Logging volume %s info and status failed",
                        cls.volname)
            return False
        g.log.info("Successful in logging volume %s info and status",
                   cls.volname)

        return True

    @classmethod
    def mount_volume(cls, mounts):
        """Mount volume
        Args:
            mounts(list): List of mount_objs
        Returns (bool): True if mounting the volume for a mount obj is
            successful. False otherwise
        """
        g.log.info("Starting to mount volume %s", cls.volname)
        for mount_obj in mounts:
            g.log.info("Mounting volume '%s:%s' on '%s:%s'",
                       mount_obj.server_system, mount_obj.volname,
                       mount_obj.client_system, mount_obj.mountpoint)
            ret = mount_obj.mount()
            if not ret:
                g.log.error("Failed to mount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)
                return False
            else:
                g.log.info("Successful in mounting volume '%s:%s' on "
                           "'%s:%s'", mount_obj.server_system,
                           mount_obj.volname, mount_obj.client_system,
                           mount_obj.mountpoint)
        g.log.info("Successful in mounting all mount objs for the volume %s",
                   cls.volname)

        # Get mounts info
        g.log.info("Get mounts Info:")
        log_mounts_info(mounts)

        return True

    @classmethod
    def setup_volume_and_mount_volume(cls, mounts, volume_create_force=False):
        """Setup the volume and mount the volume
        Args:
            mounts(list): List of mount_objs
            volume_create_force(bool): True if create_volume should be
                executed with 'force' option.
        Returns (bool): True if setting up volume and mounting the volume
            for a mount obj is successful. False otherwise
        """
        # Setup Volume
        _rc = cls.setup_volume(volume_create_force)
        if not _rc:
            return _rc

        # Mount Volume
        _rc = cls.mount_volume(mounts)
        if not _rc:
            return _rc

        return True

    @classmethod
    def unmount_volume(cls, mounts):
        """Unmount all mounts for the volume
        Args:
            mounts(list): List of mount_objs
        Returns (bool): True if unmounting the volume for a mount obj is
            successful. False otherwise
        """
        # Unmount volume
        g.log.info("Starting to UnMount Volume %s", cls.volname)
        for mount_obj in mounts:
            g.log.info("UnMounting volume '%s:%s' on '%s:%s'",
                       mount_obj.server_system, mount_obj.volname,
                       mount_obj.client_system, mount_obj.mountpoint)
            ret = mount_obj.unmount()
            if not ret:
                g.log.error("Failed to unmount volume '%s:%s' on '%s:%s'",
                            mount_obj.server_system, mount_obj.volname,
                            mount_obj.client_system, mount_obj.mountpoint)

                # Get mounts info
                g.log.info("Get mounts Info:")
                log_mounts_info(cls.mounts)

                return False
            else:
                g.log.info("Successful in unmounting volume '%s:%s' on "
                           "'%s:%s'", mount_obj.server_system,
                           mount_obj.volname, mount_obj.client_system,
                           mount_obj.mountpoint)
        g.log.info("Successful in unmounting all mount objs for the volume %s",
                   cls.volname)

        # Get mounts info
        g.log.info("Get mounts Info:")
        log_mounts_info(mounts)

        return True

    @classmethod
    def cleanup_volume(cls):
        """Cleanup the volume
        Returns (bool): True if cleanup volume is successful. False otherwise.
        """
        g.log.info("Cleanup Volume %s", cls.volname)
        ret = cleanup_volume(mnode=cls.mnode, volname=cls.volname)
        if not ret:
            g.log.error("cleanup of volume %s failed", cls.volname)
        else:
            g.log.info("Successfully cleaned-up volume %s", cls.volname)

        # Log Volume Info and Status
        g.log.info("Log Volume %s Info and Status", cls.volname)
        log_volume_info_and_status(cls.mnode, cls.volname)

        return ret

    @classmethod
    def unmount_volume_and_cleanup_volume(cls, mounts):
        """Unmount the volume and cleanup volume
        Args:
            mounts(list): List of mount_objs
        Returns (bool): True if unmounting the volume for the mounts and
            cleaning up volume is successful. False otherwise
        """
        # UnMount Volume
        _rc = cls.unmount_volume(mounts)
        if not _rc:
            return _rc

        # Setup Volume
        _rc = cls.cleanup_volume()
        if not _rc:
            return _rc
        return True

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for testing Gluster
        """
        # Set the values of servers, clients, servers_info and clients_info
        cls.servers = set_conf_entity('servers')
        cls.clients = set_conf_entity('clients')
        cls.all_servers_info = set_conf_entity('servers_info')
        cls.all_clients_info = set_conf_entity('clients_info')

        # Set mnode : Node on which gluster commands are executed
        cls.mnode = cls.servers[0]

        # Server IP's
        cls.servers_ips = []
        cls.servers_ips = get_ip_from_hostname(cls.servers)

        # Get the volume configuration
        (cls.default_volume_type_config, cls.volume_create_force,
         cls.volume, cls.voltype, cls.volname, cls.mnode) = configure_volumes(
             cls.servers, cls.volume_type)

        # Get the mount configuration.
        cls.clients, cls.mounts_dict_list, cls.mounts = configure_mounts(
            cls.mnode, cls.volname, cls.mount_type, cls.all_clients_info)

        # Get gluster Logs info
        (cls.server_gluster_logs_dirs, cls.server_gluster_logs_files,
         cls.client_gluster_logs_dirs, cls.client_gluster_logs_files,
         cls.glustotest_run_id) = configure_logs()

        msg = "Setupclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)
        inject_msg_in_gluster_logs(
            msg, cls.servers, cls.clients,
            cls.mount_type, cls.server_gluster_logs_dirs,
            cls.server_gluster_logs_files,
            cls.client_gluster_logs_dirs,
            cls.client_gluster_logs_dirs)

        # Log the baseclass variables for debugging purposes
        g.log.debug("GlusterBaseClass Variables:\n %s", cls.__dict__)

    def setUp(self):
        msg = "Starting Test : %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)
        inject_msg_in_gluster_logs(
            msg, self.servers, self.clients,
            self.mount_type, self.server_gluster_logs_dirs,
            self.server_gluster_logs_files,
            self.client_gluster_logs_dirs,
            self.client_gluster_logs_dirs)

    def tearDown(self):
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)
        inject_msg_in_gluster_logs(
            msg, self.servers, self.clients,
            self.mount_type, self.server_gluster_logs_dirs,
            self.server_gluster_logs_files,
            self.client_gluster_logs_dirs,
            self.client_gluster_logs_dirs)

    @classmethod
    def tearDownClass(cls):
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)
        inject_msg_in_gluster_logs(
            msg, cls.servers, cls.clients,
            cls.mount_type, cls.server_gluster_logs_dirs,
            cls.server_gluster_logs_files,
            cls.client_gluster_logs_dirs,
            cls.client_gluster_logs_dirs)
