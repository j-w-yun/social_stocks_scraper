# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import json
import requests
import time
from stem import Signal
from stem.control import Controller

from logger import Logger


L = Logger()
L.set_log_type('HEADER')


class Tor:
    def __init__(self, config_file='config.json', verbose=True):
        self.config_file = config_file
        self.verbose = verbose
        self.is_tor_renewing = False
        self.tor_label = ' [{}]:\t'.format('Tor')
        self.ok = requests.codes.ok

        # Read configuration from file
        try:
            with open(self.config_file) as f:
                config = json.load(f)
            self.password = config['tor_password']
            self.tor_port = config['tor_port']
            self.tor_controller_port = config['tor_controller_port']
        except Exception as e:
            if self.verbose:
                L.log(self.tor_label, 'Failed to read {}'.format(self.config_file), e)
            raise Exception('Failed to read {}'.format(self.config_file))

    def get_session(self, renew=False):
        """Use the tor network as a proxy.
        """
        if renew:
            self.renew_connection()

        while self.is_tor_renewing:
            time.sleep(0.1)

        # if self.verbose:
        #     L.log(self.tor_label, 'Getting session')

        session = requests.session()
        session.proxies = {
            'http': 'socks5://127.0.0.1:{}'.format(self.tor_port),
            'https': 'socks5://127.0.0.1:{}'.format(self.tor_port),
        }
        return session

    def renew_connection(self):
        """Establish a clean pathway through the tor network.
        """
        while self.is_tor_renewing:
            time.sleep(0.1)

        if self.verbose:
            L.log(self.tor_label, 'Renewing connection')

        self.is_tor_renewing = True
        with Controller.from_port(port=self.tor_controller_port) as c:
            c.authenticate(password=self.password)
            time.sleep(c.get_newnym_wait())
            c.signal(Signal.NEWNYM)
            # time.sleep(c.get_newnym_wait())
        self.is_tor_renewing = False
