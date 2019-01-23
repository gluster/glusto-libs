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
from glustolibs.gluster.lib_utils import (
    get_ip_from_hostname, configure_volumes,
    configure_mounts, inject_msg_in_gluster_logs,
    configure_logs, set_conf_entity)


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
