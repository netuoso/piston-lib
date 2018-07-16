import re
import sys
import threading
import websocket
import ssl
import json
import time
import socket
import certifi
import pdb
from urllib3 import PoolManager
from urllib3.connection import HTTPConnection
from itertools import cycle
from . import exceptions
from .exceptions import NoAccessApi, RPCError
from pistonbase.chains import known_chains
import logging
import warnings
warnings.filterwarnings('default', module=__name__)
log = logging.getLogger(__name__)


class SteemNodeRPC(object):
    """ This class allows to call API methods synchronously, without
        callbacks. It logs in and registers to the APIs:

        * database
        * history

        :param str urls: Either a single Websocket URL, or a list of URLs
        :param str user: Username for Authentication
        :param str password: Password for Authentication
        :param Array apis: List of APIs to register to (default: ["database", "network_broadcast"])

        Available APIs

              * database
              * network_node
              * network_broadcast
              * history

    """
    call_id = 0
    api_id = {}

    def __init__(self,
                 urls,
                 user="",
                 password="",
                 **kwargs):
        self.apis = kwargs.pop(
            "apis",
            ["database", "network_broadcast"]
        )

        self.api_id = {}
        self._request_id = 0
        if isinstance(urls, list):
            self.urls = cycle(urls)
        else:
            self.urls = cycle([urls])
        self.user = user
        self.password = password
        self.num_retries = kwargs.get("num_retries", -1)
        self.is_http = False

        self.connect()
        self.register_apis()

        self.chain_params = self.get_network()

    def get_request_id(self):
        self._request_id += 1
        return self._request_id

    def connect(self):
        cnt = 0
        while True:
            cnt += 1
            self.url = next(self.urls)
            log.debug("Trying to connect to node %s" % self.url)
            

            if self.url[:4] == "http":
                #pdb.set_trace()
                if not self.is_http:
                    self.http = PoolManager(
                            timeout = 60,
                            retries = 20,
                            socket_options = HTTPConnection.default_socket_options + \
                                 [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), ],
                                     headers={'Content-Type': 'application/json'},
                                     cert_reqs='CERT_REQUIRED',
                                     ca_certs=certifi.where())
                    self.is_http = True
                break

            if self.url[:3] == "wss":
                sslopt_ca_certs = {'cert_reqs': ssl.CERT_NONE}
                self.ws = websocket.WebSocket(sslopt=sslopt_ca_certs)
            else:
                self.ws = websocket.WebSocket()
            try:
                self.is_http = False
                self.ws.connect(self.url)
                break
            except KeyboardInterrupt:
                raise
            except:
                if (self.num_retries >= 0 and cnt > self.num_retries):
                    raise NumRetriesReached()

                sleeptime = (cnt - 1) * 2 if cnt < 10 else 10
                if sleeptime:
                    log.warning(
                        "Lost connection to node during connect(): %s (%d/%d) "
                        % (self.url, cnt, self.num_retries) +
                        "Retrying in %d seconds" % sleeptime
                    )
                    time.sleep(sleeptime)
        if not self.is_http:
            self.login(self.user, self.password, api_id=1)


    def register_apis(self, apis=None):
        if self.is_http:
            return
        for api in (apis or self.apis):
            api = api.replace("_api", "")
            self.api_id[api] = self.get_api_by_name("%s_api" % api, api_id=1)
            if not self.api_id[api] and not isinstance(self.api_id[api], int):
                raise NoAccessApi("No permission to access %s API. " % api)

    def get_account(self, name):
        account = self.get_accounts([name])
        if account:
            return account[0]

    def account_history(self, account, first=99999999999,
                        limit=-1, only_ops=[], exclude_ops=[]):
        warnings.warn(
            "The block_stream() call has been moved to `steem.account.Account.rawhistory()`",
            DeprecationWarning
        )
        from piston.account import Account
        return Account(account, steem_instance=self.steem).rawhistory(
            first=first, limit=limit,
            only_ops=only_ops,
            exclude_ops=exclude_ops)

    def block_stream(self, start=None, stop=None, mode="irreversible"):
        warnings.warn(
            "The block_stream() call has been moved to `steem.blockchain.Blockchain.blocks()`",
            DeprecationWarning
        )
        from piston.blockchain import Blockchain
        return Blockchain(mode=mode).blocks(start, stop)

    def stream(self, opNames, *args, **kwargs):
        warnings.warn(
            "The stream() call has been moved to `steem.blockchain.Blockchain.stream()`",
            DeprecationWarning
        )
        from piston.blockchain import Blockchain
        return Blockchain(mode=kwargs.get("mode", "irreversible")).stream(opNames, *args, **kwargs)

    def list_accounts(self, start=None, step=1000, limit=None, **kwargs):
        warnings.warn(
            "The list_accounts() call has been moved to `steem.blockchain.Blockchain.get_all_accounts()`",
            DeprecationWarning
        )
        from piston.blockchain import Blockchain
        return Blockchain(mode=kwargs.get("mode", "irreversible")).get_all_accounts(start=start, steps=step, **kwargs)

    def get_network(self):
        """ Identify the connected network. This call returns a
            dictionary with keys chain_id, prefix, and other chain
            specific settings
        """
        props = self.get_dynamic_global_properties()
        chain = props["current_supply"].split(" ")[1]
        assert chain in known_chains, "The chain you are connecting to is not supported"
        return known_chains.get(chain)

    def rpcexec(self, payload):
        """ Execute a call by sending the payload.
            In here, we mostly deal with Steem specific error handling

            :param json payload: Payload data
            :raises ValueError: if the server does not respond in proper JSON format
            :raises RPCError: if the server returns an error
        """
        try:
            return self._rpcexec(payload)
        except RPCError as e:
            msg = exceptions.decodeRPCErrorMsg(e).strip()
            if msg == "Account already transacted this block.":
                raise exceptions.AlreadyTransactedThisBlock(msg)
            elif msg == "missing required posting authority":
                raise exceptions.MissingRequiredPostingAuthority
            elif msg == "Voting weight is too small, please accumulate more voting power or steem power.":
                raise exceptions.VoteWeightTooSmall(msg)
            elif msg == "Can only vote once every 3 seconds.":
                raise exceptions.OnlyVoteOnceEvery3Seconds(msg)
            elif msg == "You have already voted in a similar way.":
                raise exceptions.AlreadyVotedSimilarily(msg)
            elif msg == "You may only post once every 5 minutes.":
                raise exceptions.PostOnlyEvery5Min(msg)
            elif msg == "Duplicate transaction check failed":
                raise exceptions.DuplicateTransaction(msg)
            elif msg == "Account exceeded maximum allowed bandwidth per vesting share.":
                raise exceptions.ExceededAllowedBandwidth(msg)
            elif re.match("^no method with name.*", msg):
                raise exceptions.NoMethodWithName(msg)
            elif msg:
                raise exceptions.UnhandledRPCError(msg)
            else:
                raise e
        except Exception as e:
            raise e


    """ RPC Calls
    """
    def _rpcexec(self, payload):
        """ Execute a call by sending the payload

            :param json payload: Payload data
            :raises ValueError: if the server does not respond in proper JSON format
            :raises RPCError: if the server returns an error
        """
        log.debug(json.dumps(payload))
        cnt = 0
        while True:
            cnt += 1

            try:
                if self.is_http:
                    _body = json.dumps(payload)
                    response = self.http.urlopen('POST', self.url, body = _body)
                    reply = response.data.decode('utf-8') 
                else:
                    self.ws.send(json.dumps(payload, ensure_ascii=False).encode('utf8'))
                    reply = self.ws.recv()
                break
                
            except KeyboardInterrupt:
                raise
            except:
                log.warning(str(sys.exc_info()))
                if (self.num_retries > -1 and
                        cnt > self.num_retries):
                    raise NumRetriesReached()
                sleeptime = (cnt - 1) * 2 if cnt < 10 else 10
                if sleeptime:
                    log.warning(
                        "Lost connection to node during rpcexec(): %s (%d/%d) "
                        % (self.url, cnt, self.num_retries) +
                        "Retrying in %d seconds" % sleeptime 
                    )
                    time.sleep(sleeptime)

                # retry
                try:
                    if not self.is_http:
                        self.ws.close()
                    time.sleep(sleeptime)
                    self.connect()
                    self.register_apis()
                except:
                    pass

        ret = {}
        try:
            ret = json.loads(reply, strict=False)
        except ValueError:
            raise ValueError("Client returned invalid format. Expected JSON!")

        log.debug(json.dumps(reply))

        if 'error' in ret:
            print(ret)
            if 'detail' in ret['error']:
                raise RPCError(ret['error']['detail'])
            else:
                raise RPCError(ret['error']['message'])
        else:
            return ret["result"]


    def __getattr__(self, name):
        """ Map all methods to RPC calls and pass through the arguments.
        """
        def method(*args, **kwargs):

            # Sepcify the api to talk to
            if "api_id" not in kwargs:
                if ("api" in kwargs):
                    if (kwargs["api"] in self.api_id and
                            self.api_id[kwargs["api"]]):
                        api_id = self.api_id[kwargs["api"]]
                    elif self.is_http and kwargs["api"]:
                        api_id = kwargs["api"]+"_api"
                    else:
                        raise ValueError(
                            "Unknown API! "
                            "Verify that you have registered to %s"
                            % kwargs["api"]
                        )
                elif self.is_http:
                    api_id = "database_api" 
                else:
                    api_id = 0 
            else:
                api_id = kwargs["api_id"]

            # let's be able to define the num_retries per query
            self.num_retries = kwargs.get("num_retries", self.num_retries)

            query = {"method": "call",
                     "params": [api_id, name, list(args)],
                     "jsonrpc": "2.0",
                     "id": self.get_request_id()}
            r = self.rpcexec(query)
            return r
        return method
