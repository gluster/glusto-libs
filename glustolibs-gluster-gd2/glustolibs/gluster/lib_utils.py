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
    Description: Helper library for gluster modules.
"""

from glusto.core import Glusto as g


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
