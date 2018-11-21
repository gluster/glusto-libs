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
    Description: Library to handle all the common REST methods like GET, POST
    & DELETE
"""

import json
import datetime
import hashlib
import jwt
import requests
from glusto.core import Glusto as g


class RestClient(object):
    '''
        Class contains common methods for POST, GET, DELETE and
        verifies authentication
    '''
    def __init__(self, mnode, port='24007', user='glustercli', secret=None,
                 verify=False):
        """
        Function to form base url and to get secret key

        Args:
            mnode (str): The server on which the command has to be executed
            port (str) : The port which API calls use
            user (str) : By default the user is glustercli
        """

        self.user = user
        self.mnode = mnode
        self.secret = secret
        self.verify = verify
        self.port = port
        self.base_url = ('http://{mnode}:{port}'.format(mnode=mnode,
                                                        port=port))
        if self.secret is None:
            _, self.secret, _ = g.run(mnode, "cat /var/lib/glusterd2/auth")

    def _set_token_in_header(self, method, url, headers=None):
        """
        Function to set token in header

        Args:
           method (str): It can be GET, POST or DELETE
           url (str): The url for operation

        For Example:
          token =  _set_token_in_header('GET', '/v1/peers')

        """

        if headers is None:
            headers = dict()
        claims = dict()
        claims['iss'] = self.user

        # Issued at time
        claims['iat'] = datetime.datetime.utcnow()

        # Expiration time
        claims['exp'] = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=1)

        # URI tampering protection
        val = b'%s&%s' % (method.encode('utf8'), url.encode('utf8'))
        claims['qsh'] = hashlib.sha256(val).hexdigest()

        token = jwt.encode(claims, self.secret, algorithm='HS256')
        headers['Authorization'] = b'bearer ' + token

        return headers

    def handle_request(self, method, url, expected_status_code, data=None):
        """ Function that handles all the methods(GET, POST, DELETE)

        Args:
            method (str): It can be GET, POST, DELETE
            url (str): The url of the operation
            expected_status_code (str) : The status_code expected after
                                      the API call
            data (str): The json input that needs to be passed

        Returns:
            tuple: Tuple containing three elements (ret, out, err).
                The first element 'ret' is of type 'int' and returns the status
                code of command execution.

                The second element 'out' is of type 'str' and is the
                stdout value

                The third element 'err' is of type 'str' and is the
                stderr message|value of the command execution.

        Example:
            handle_request('GET', "/v1/volumes", '200')
            handle_request('POST', "/vi/volumes", '201', data)
        """

        headers = self._set_token_in_header(method, url)
        resp = requests.request(method, self.base_url + url,
                                data=json.dumps(data),
                                headers=headers, verify=self.verify)

        if resp.status_code != expected_status_code:
            return (resp.status_code, None, json.dumps(resp.json()))

        if resp.status_code == 204:
            return resp.status_code, {}

        return (resp.status_code, json.dumps(resp.json()), None)

