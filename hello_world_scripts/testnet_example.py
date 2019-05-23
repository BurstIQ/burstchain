#!/usr/bin/env python3

"""
Copyright (c) 2015-2019 BurstIQ Analytics Corporation - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited.
Proprietary and confidential
"""

import argparse
import datetime
import json
import logging
import random
from typing import List, Tuple

import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] [%(processName)s] [%(threadName)-10s] : %(message)s')

logger = logging.getLogger(__name__)


class BurstChainClient(object):
    """
    A simple client for BursChain; allows the caller to make programmatic function calls for rest
    base server API
    """

    def __init__(self, server: str, client: str):
        self._server = server
        self._client = client
        self._session = requests.Session()
        # disables ~/.netrc
        self._session.trust_env = False

    def _send(self,
              action: str,
              path: str,
              addl_headers: [None, dict],
              body: [None, dict],
              auth: [None, Tuple]) -> [None, dict]:
        """
        performs the base functionality for making http REST calls
        :param action:
        :param path:
        :param addl_headers:
        :param body:
        :param auth:
        :return:
        """
        headers = {'Content-Type': 'application/json', 'Content-Accept': 'application/json'}
        if addl_headers:
            headers.update(addl_headers)

        response = self._session.request(action, f'{self._server}{path}', headers=headers, json=body, auth=auth)

        if response.status_code != 200:
            logger.error(f'Call for {path} failed with code {response.status_code} and response of {response.text}')
            exit(1)

        if response.text:
            return json.loads(response.text)
        else:
            return None

    def _send_with_priv_id(self,
                           action: str,
                           path: str,
                           priv_id: str,
                           body: [None, dict]) -> [None, dict]:
        """
        builds the proper header for a private id field, vs basic auth
        :param action:
        :param path:
        :param priv_id:
        :param body:
        :return:
        """
        if priv_id:
            headers = {'Authorization': f'ID {priv_id}'}
        else:
            headers = None

        return self._send(action, path, headers, body, None)

    def put_metadata(self,
                     dictionary: dict,
                     username: str,
                     password: str) -> dict:
        """
        Puts the dictionary into the server
        :param dictionary:
        :param username:
        :param password:
        :return: response
        """
        return self._send('PUT', '/api/metadata/dictionary', None, dictionary, (username, password))

    def get_private_id(self) -> str:
        """
        get a new private id from the burst chain
        :return: private id
        """
        resp = self._send('GET', '/api/burstchain/id/private', None, None, None)
        return resp.get('private_id')

    def get_public_id(self, private_id: str) -> str:
        """
        get a new public id that is paired with this private id from the burst chain
        :return: public id
        """
        resp = self._send_with_priv_id('GET', '/api/burstchain/id/public', private_id, None)
        return resp.get('public_id')

    def create_asset(self,
                     private_id: str,
                     chain_name: str,
                     owners: List[str],
                     asset: dict,
                     asset_metadata: [None, dict]) -> str:
        """
        creates a new asset
        :param private_id:
        :param chain_name:
        :param owners:
        :param asset:
        :param asset_metadata:
        :return: the asset id
        """
        body = {
            'owners': owners,
            'asset': asset,
            'asset_metadata': asset_metadata
        }

        resp = self._send_with_priv_id('POST',
                                       f'/api/burstchain/{self._client}/{chain_name}/asset',
                                       private_id, body)
        return resp.get('asset_id')

    def get_asset_status(self, private_id: str, chain_name: str, asset_id: str) -> str:
        """
        gets the chains status of a specific asset
        :param private_id:
        :param chain_name:
        :param asset_id:
        :return:
        """
        resp = self._send_with_priv_id('GET',
                                       f'/api/burstchain/{self._client}/{chain_name}/{asset_id}/status',
                                       private_id, None)
        return resp.get('message')

    def get_asset_by_id(self,
                        private_id: str,
                        chain_name: str,
                        asset_id: str) -> [None, dict]:
        """

        :param private_id:
        :param chain_name:
        :param asset_id:
        :return:
        """
        return self._send_with_priv_id('GET',
                                       f'/api/burstchain/{self._client}/{chain_name}/{asset_id}/latest',
                                       private_id,
                                       None)

    def get_asset_by_hash(self,
                          private_id: str,
                          chain_name: str,
                          hash_value: str) -> [None, dict]:
        """

        :param private_id:
        :param chain_name:
        :param hash_value:
        :return:
        """
        return self._send_with_priv_id('GET',
                                       f'/api/burstchain/{self._client}/{chain_name}/{hash_value}',
                                       private_id,
                                       None)

    def update_asset(self,
                     private_id: str,
                     chain_name: str,
                     asset_id: str,
                     asset: dict,
                     asset_metadata: [None, dict]) -> str:
        """

        :param private_id:
        :param chain_name:
        :param asset_id:
        :param asset:
        :param asset_metadata:
        :return:
        """
        body = {
            'asset_id': asset_id,
            'asset': asset,
            'asset_metadata': asset_metadata
        }

        resp = self._send_with_priv_id('PUT',
                                       f'/api/burstchain/{self._client}/{chain_name}/asset',
                                       private_id,
                                       body)
        return resp.get('asset_id')

    def transfer_asset(self,
                       private_id: str,
                       chain_name: str,
                       asset_id: str,
                       owners: List[str],
                       new_owners: List[str],
                       new_signer_public_id: str) -> str:
        body = {
            'asset_id': asset_id,
            'owners': owners,
            'new_owners': new_owners,
            'new_signer_public_id': new_signer_public_id
        }

        resp = self._send_with_priv_id('POST',
                                       f'/api/burstchain/{self._client}/{chain_name}/transfer',
                                       private_id,
                                       body)
        return resp.get('asset_id')

    def query(self,
              private_id: str,
              chain_name: str,
              tql: str) -> [None, List[dict]]:
        body = {
            'tqlWhereClause': tql
        }

        resp = self._send_with_priv_id('POST',
                                       f'/api/burstchain/{self._client}/{chain_name}/query',
                                       private_id,
                                       body)
        return resp.get('assets')

    def map_reduce(self,
                   private_id: str,
                   chain_name: str,
                   map_func: str,
                   reduce_func: str,
                   finalize_func: [None, str],
                   tql: str) -> List[dict]:
        body = {
            'map': map_func,
            'reduce': reduce_func,
            'finalize': finalize_func,
            'tqlWhereClause': tql
        }
        resp = self._send_with_priv_id('POST',
                                       f'/api/burstchain/{self._client}/{chain_name}/mapreduce/query',
                                       private_id,
                                       body)
        return resp.get('records')

    def create_consent(self,
                       private_id: str,
                       chain_name: str,
                       owners: List[str],
                       contract_name: str,
                       contract: str) -> str:
        body = {
            'contract': contract,
            'name': contract_name,
            'smart_contract_metadata': {'loaded by': 'hello world demo'},
            'smart_contract_type': 'consent',
            'owners': owners
        }

        logger.info(json.dumps(body))

        resp = self._send_with_priv_id('POST',
                                       f'/api/burstchain/{self._client}/{chain_name}/smartcontract',
                                       private_id,
                                       body)
        return resp.get('asset_id')


def parse_args() -> argparse.Namespace:
    """
    cmd line arguments parser for the demo
    :return:
    """
    _DEFAULT_SERVER = 'https://testnet.burstiq.com'

    # build command line arguments parser with command string
    args = argparse.ArgumentParser()

    # db args
    args.add_argument('-s', '--server',
                      dest='server',
                      default=_DEFAULT_SERVER,
                      help=f'The testnet server; defaults to {_DEFAULT_SERVER}')
    args.add_argument('-c', '--client',
                      dest='client',
                      required=True,
                      help='The client name to interact with')
    args.add_argument('-u', '--username',
                      dest='username',
                      required=True,
                      help='The username that is the admin account for the client space')
    args.add_argument('-p', '--password',
                      dest='password',
                      required=True,
                      help='The username password above')
    args.add_argument('--privateid',
                      dest='privateid',
                      help='An existing private id to use instead of generating a new one')
    args.add_argument('-q', '--quiet',
                      dest='loglevel', action='store_const',
                      const=logging.WARN, default=logging.INFO,
                      help='log only warnings')
    args.add_argument('-v', '--verbose',
                      dest='loglevel', action='store_const',
                      const=logging.DEBUG, default=logging.INFO,
                      help='log low level details')

    # parse command line args
    return args.parse_args()


def main():
    logger.info('\n'
                + '--------------------------------------------------------------\n'
                + 'Burst Chain Client - Hello World Demo\n'
                + 'Copyright (c) 2015-2019 BurstIQ Analytics Corporation\n'
                + '--------------------------------------------------------------\n')

    # parse the cmd line args
    opts = parse_args()

    # reset log level
    logging.getLogger().setLevel(opts.loglevel)
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        from http.client import HTTPConnection
        HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    # create the burst chain client
    client = BurstChainClient(opts.server, opts.client)

    # ------------------------------------------------------------------------
    # STEP 1 - Setup the dictionary for a chain
    #
    dictionary = {
        'collection': 'address',

        'indexes': [{
            'unique': True,
            'attributes': ['id']
        }],

        'rootnode': {
            'attributes': [{
                'name': 'id',
                'required': True
            }, {
                'name': 'addr1'
            }, {
                'name': 'addr2'
            }, {
                'name': 'city'
            }, {
                'name': 'state'
            }, {
                'name': 'zip'
            }]
        }
    }

    resp = client.put_metadata(dictionary, opts.username, opts.password)
    logger.info(f"PUT dictionary response: {resp.get('message')}")

    # ------------------------------------------------------------------------
    # STEP 2 - get a private id
    #
    if opts.privateid:
        private_id = opts.privateid
    else:
        private_id = client.get_private_id()
    logger.info(f'Using private id {private_id} for this demo')

    # ------------------------------------------------------------------------
    # STEP 3 - get the public id
    #
    public_id = client.get_public_id(private_id)
    logger.info(f'Using public id {public_id} for this demo')

    # ------------------------------------------------------------------------
    # STEP 4 - create asset for the dictionary
    #
    asset = {
        'id': f'{random.randint(1000, 10000)}',
        'addr1': '123 Main St',
        'city': 'Nowhere',
        'state': 'XX',
        'zip': '12345-0000'
    }

    asset_metadata = {'loaded by': 'hello world demo'}

    first_asset_id = client.create_asset(private_id, dictionary['collection'], [public_id], asset, asset_metadata)
    logger.info(f'Asset created {first_asset_id} for this demo')

    # ------------------------------------------------------------------------
    # STEP 5 - get status of last asset
    #
    accepted_msg = client.get_asset_status(private_id, dictionary.get('collection'), first_asset_id)
    logger.info(f'Status response message {accepted_msg}')

    # ------------------------------------------------------------------------
    # STEP 6 - get asset via id
    #
    resp = client.get_asset_by_id(private_id, dictionary.get('collection'), first_asset_id)
    first_hash = resp.get('hash')
    logger.info(f'ASSET:\n {json.dumps(resp, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 7 - get asset via hash
    #
    resp = client.get_asset_by_hash(private_id, dictionary.get('collection'), first_hash)
    logger.info(f'ASSET:\n {json.dumps(resp, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 8 - update asset
    #
    asset['state'] = 'CO'

    tmp_id = client.update_asset(private_id, dictionary.get('collection'), first_asset_id, asset, None)
    logger.info(f'Asset updated {tmp_id} for this demo')

    # ------------------------------------------------------------------------
    # STEP 9 - get asset via id (again)
    #
    resp = client.get_asset_by_id(private_id, dictionary.get('collection'), first_asset_id)
    logger.info(f'ASSET:\n {json.dumps(resp, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 10 - transfer the asset to another owner
    #
    second_private_id = client.get_private_id()
    second_public_id = client.get_public_id(second_private_id)

    resp = client.transfer_asset(private_id, dictionary.get('collection'), first_asset_id,
                                 [public_id], [second_public_id], second_public_id)
    logger.info(f'transferred asset id {resp}')

    # the original owner should NO longer be able to see this asset
    resp = client.get_asset_by_id(private_id, dictionary.get('collection'), first_asset_id)
    logger.info(f'ASSET should be NULL:\n {json.dumps(resp, indent=2)}')

    # the new owner should be able to see this asset
    resp = client.get_asset_by_id(second_private_id, dictionary.get('collection'), first_asset_id)
    logger.info(f'ASSET:\n {json.dumps(resp, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 11 - query via TQL
    #

    tql = "WHERE asset.state = 'CO'"

    assets = client.query(second_private_id, dictionary.get('collection'), tql)
    logger.info(f'TQL 1 ASSET:\n {json.dumps(assets, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 12 - query via TQL
    #
    tql = "SELECT asset.id FROM address WHERE asset.state = 'CO'"

    assets = client.query(second_private_id, dictionary.get('collection'), tql)
    logger.info(f'TQL 2 ASSET:\n {json.dumps(assets, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 13 - map_reduce
    #
    m = 'function() { emit(this.asset.state, this) }'
    r = 'function(k, v) { return v[0] }'
    q = "WHERE asset.state = 'CO' ORDER BY asset.id LIMIT 100"

    assets = client.map_reduce(second_private_id, dictionary.get('collection'), m, r, None, q)
    logger.info(f'MR ASSET:\n {json.dumps(assets, indent=2)}')

    # ------------------------------------------------------------------------
    # STEP 14 - consent contract
    #
    end_date = datetime.datetime.now() + datetime.timedelta(days=10)

    c = f"consents {public_id} " \
        f"for {dictionary.get('collection')} " \
        "when asset.state = 'CO' " \
        f"until Date('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')"

    tmp_id = client.create_consent(second_private_id, dictionary.get('collection'), [second_private_id],
                                   'first consent', c)
    logger.info(f'Smart Contract (consent) asset id {tmp_id} for this demo')

    # the original owner should BE be able to see this asset now with the consent contract in place
    resp = client.get_asset_by_id(private_id, dictionary.get('collection'), first_asset_id)
    logger.info(f'ASSET should be VIEWABLE:\n {json.dumps(resp, indent=2)}')


if __name__ == '__main__':
    main()
