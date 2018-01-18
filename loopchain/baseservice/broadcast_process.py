# Copyright 2017 theloop, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""gRPC broadcast process"""

import json
import logging
import pickle
import queue
import time
from enum import Enum

from loopchain import configure as conf
from loopchain.baseservice import ManageProcess, StubManager, PeerManager
from loopchain.blockchain.transaction import Transaction
from loopchain.protos import loopchain_pb2, loopchain_pb2_grpc, message_code


class PeerProcessStatus(Enum):
    normal = 0
    leader_complained = 1


class TxItem:
    def __init__(self, tx_dump: bytes, channel_name: str):
        self.__tx_dump = tx_dump
        self.__channel_name = channel_name

    @property
    def tx_dump(self):
        return self.__tx_dump

    @property
    def channel_name(self):
        return self.__channel_name


class BroadcastProcess(ManageProcess):
    """broadcast class for 'tx_process' and 'broadcast_process'
    One process class has two reason. (Run as two processes shared one code)
    two processes is more stable than one.
    It's a separated process from main peer process, so It has own peer list that called audience.
    """
    PROCESS_INFO_KEY = "process_info"
    PROCESS_VARIABLE_STUB_TO_SELF_PEER = "stub_to_self_peer"
    PROCESS_VARIABLE_PEER_STATUS = "peer_status"

    SELF_PEER_TARGET_KEY = "self_peer_target"
    LEADER_PEER_TARGET_KEY = "leader_peer_target"
    SUBSCRIBE_COMMAND = "subscribe"
    UNSUBSCRIBE_COMMAND = "unsubscribe"
    UPDATE_AUDIENCE_COMMAND = "update_audience"
    BROADCAST_COMMAND = "broadcast"
    MAKE_SELF_PEER_CONNECTION_COMMAND = "make_self_connection"
    CONNECT_TO_LEADER_COMMAND = "connect_to_leader"
    CREATE_TX_COMMAND = "create_tx"
    STATUS_COMMAND = "status"

    def __init__(self, process_name="Broadcast Process"):
        ManageProcess.__init__(self)
        self.__process_name = process_name

    def process_loop(self, manager_dic, manager_list):
        logging.info(f"({self.__process_name}) Start.")

        # for bloadcast(announce) peer Dic ( key=peer_target, value=stub(gRPC) )
        __audience = {}

        command = None
        stored_tx = queue.Queue()
        __process_variables = dict()
        __process_variables[self.PROCESS_VARIABLE_PEER_STATUS] = PeerProcessStatus.normal

        def __broadcast_tx(stored_tx_item: TxItem):
            # logging.debug(f"({self.__process_name}): broadcast tx audience({len(__audience)})")
            result_add_tx = None

            for peer_target in list(__audience):
                # logging.debug("peer_target: " + peer_target)
                stub_item = __audience[peer_target]
                stub_item.call_async(
                    "AddTx", loopchain_pb2.TxSend(
                        tx=stored_tx_item.tx_dump,
                        channel=stored_tx_item.channel_name)
                )

            return result_add_tx

        def create_tx_continue():
            # 저장된 작업이 있으면 전송한다.
            while not stored_tx.empty():
                stored_tx_item = stored_tx.get()
                __broadcast_tx(stored_tx_item)

        def __broadcast_run(method_name, method_param):
            """call gRPC interface of audience

            :param method_name: gRPC interface
            :param method_param: gRPC message
            """
            # logging.debug(f"...do broadcast run... ({len(__audience)})")

            for peer_target in list(__audience):
                # logging.debug("peer_target: " + peer_target)
                stub_item = __audience[peer_target]
                stub_item.call_async(method_name, method_param)

        def __handler_subscribe(subscribe_peer_target):
            # logging.debug("BroadcastProcess received subscribe command peer_target: " + str(subscribe_peer_target))
            if subscribe_peer_target not in __audience:
                stub_manager = StubManager.get_stub_manager_to_server(
                    subscribe_peer_target, loopchain_pb2_grpc.PeerServiceStub,
                    time_out_seconds=conf.CONNECTION_RETRY_TIMEOUT_WHEN_INITIAL,
                    is_allow_null_stub=True
                )
                __audience[subscribe_peer_target] = stub_manager

        def __handler_unsubscribe(unsubscribe_peer_target):
            # logging.debug(f"BroadcastProcess received unsubscribe command peer_target({unsubscribe_peer_target})")
            try:
                del __audience[unsubscribe_peer_target]
            except KeyError:
                logging.warning("Already deleted peer: " + str(unsubscribe_peer_target))

        def __handler_update_audience(audience_param):
            peer_manager = PeerManager()
            peer_list_data = pickle.loads(audience_param)
            peer_manager.load(peer_list_data, False)

            for peer_id in list(peer_manager.peer_list[conf.ALL_GROUP_ID]):
                peer_each = peer_manager.peer_list[conf.ALL_GROUP_ID][peer_id]
                if peer_each.target != __process_variables[self.SELF_PEER_TARGET_KEY]:
                    logging.warning(f"broadcast process peer_targets({peer_each.target})")
                    __handler_subscribe(peer_each.target)

        def __handler_broadcast(broadcast_param):
            # logging.debug("BroadcastProcess received broadcast command")
            broadcast_method_name = broadcast_param[0]
            broadcast_method_param = broadcast_param[1]
            # logging.debug("BroadcastProcess method name: " + broadcast_method_name)
            # logging.debug("BroadcastProcess method param: " + str(broadcast_method_param))
            __broadcast_run(broadcast_method_name, broadcast_method_param)

        def __handler_status(status_param):
            logging.debug(f"({self.__process_name}) Status, param({status_param}) audience({len(__audience)})")

            status = dict()
            status['result'] = message_code.get_response_msg(message_code.Response.success)
            status['Audience'] = str(len(__audience))
            status_json = json.dumps(status)

            # return way of manage_process
            manager_dic["status"] = status_json

        def __handler_create_tx(create_tx_param):
            # logging.debug(f"({self.__process_name}) create_tx....")

            try:
                tx_item = TxItem(pickle.dumps(create_tx_param), create_tx_param.meta[Transaction.CHANNEL_KEY])
            except Exception as e:
                logging.warning(f"tx in channel({create_tx_param.meta[Transaction.CHANNEL_KEY]})")
                logging.warning(f"tx dumps fail ({e})")
                return

            if __process_variables[self.PROCESS_VARIABLE_PEER_STATUS] == PeerProcessStatus.leader_complained:
                stored_tx.put(tx_item)
                logging.warning("Leader is complained your tx just stored in queue by temporally: "
                                + str(stored_tx.qsize()))
            else:
                create_tx_continue()
                __broadcast_tx(tx_item)

        def __handler_connect_to_leader(connect_to_leader_param):
            # logging.debug("(tx process) try... connect to leader: " + str(connect_to_leader_param))
            __process_variables[self.LEADER_PEER_TARGET_KEY] = connect_to_leader_param

            # stub_to_self_peer = __process_variables[self.PROCESS_VARIABLE_STUB_TO_SELF_PEER]

            __process_variables[self.PROCESS_VARIABLE_PEER_STATUS] = PeerProcessStatus.normal

        def __handler_connect_to_self_peer(connect_param):
            # 자신을 생성한 부모 Peer 에 접속하기 위한 stub 을 만든다.
            # pipe 를 통한 return 은 pipe send 와 쌍이 맞지 않은 경우 오류를 발생시킬 수 있다.
            # 안전한 연결을 위하여 부모 프로세스와도 gRPC stub 을 이용하여 통신한다.
            logging.debug("try connect to self peer: " + str(connect_param))

            stub_to_self_peer = StubManager.get_stub_manager_to_server(
                connect_param, loopchain_pb2_grpc.InnerServiceStub,
                time_out_seconds=conf.CONNECTION_RETRY_TIMEOUT_WHEN_INITIAL,
                is_allow_null_stub=True
            )
            __process_variables[self.SELF_PEER_TARGET_KEY] = connect_param
            __process_variables[self.PROCESS_VARIABLE_STUB_TO_SELF_PEER] = stub_to_self_peer

        __handler_map = {
            self.CREATE_TX_COMMAND: __handler_create_tx,
            self.CONNECT_TO_LEADER_COMMAND: __handler_connect_to_leader,
            self.SUBSCRIBE_COMMAND: __handler_subscribe,
            self.UNSUBSCRIBE_COMMAND: __handler_unsubscribe,
            self.UPDATE_AUDIENCE_COMMAND: __handler_update_audience,
            self.BROADCAST_COMMAND: __handler_broadcast,
            self.MAKE_SELF_PEER_CONNECTION_COMMAND: __handler_connect_to_self_peer,
            self.STATUS_COMMAND: __handler_status
        }

        while command != ManageProcess.QUIT_COMMAND:

            # logging.debug(f"manager list: {manager_list}")
            try:
                if not manager_list:
                    # logging.debug(f"manager list: {manager_list}")
                    time.sleep(conf.SLEEP_SECONDS_IN_SERVICE_LOOP)
                else:
                    # logging.debug("BroadcastProcess manage_list is not  empty")
                    # logging.debug(f"manager list: {manager_list}")
                    # logging.debug(f"manager list object: {id(manager_list)}")
                    # logging.info(f'if not manager_list: {not manager_list}')
                    # logging.info(f'if manager_list is not None: {manager_list is not None}')
                    # logging.info(f'if manager: {len(manager_list)}')

                    # packet must be a tuple (command, param)
                    command, param = manager_list.pop()

                    if command in __handler_map.keys():
                        # logging.debug(f'Broadcast Process do {command}')
                        __handler_map[command](param)
                        continue

                    if command == ManageProcess.QUIT_COMMAND:
                        logging.debug(f"({self.__process_name}) "
                                      f"peer({manager_dic[self.PROCESS_INFO_KEY]}) will quit soon.")
                    else:
                        logging.error(f"({self.__process_name}) received Unknown command: " +
                                      str(command) + " and param: " + str(param))
            except Exception as e:
                time.sleep(conf.SLEEP_SECONDS_IN_SERVICE_LOOP)
                logging.error(f"({self.__process_name}) not available reason({e})")
                break

        logging.info(f"({self.__process_name}) Ended.")
