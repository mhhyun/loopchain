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
"""A management class for blockchain."""

import os.path as osp
import queue
import shutil
import uuid

from loopchain.baseservice import CommonThread, ObjectManager, Timer
from loopchain.blockchain import *
from loopchain.peer.candidate_blocks import CandidateBlocks
from loopchain.peer.consensus_default import ConsensusDefault
from loopchain.peer.consensus_lft import ConsensusLFT
from loopchain.peer.consensus_none import ConsensusNone
from loopchain.peer.consensus_siever import ConsensusSiever

import loopchain_pb2


class BlockManager(CommonThread):
    """P2P Service 를 담당하는 BlockGeneratorService, PeerService 와 분리된
    Thread 로 BlockChain 을 관리한다.
    BlockGenerator 의 BlockManager 는 주기적으로 Block 을 생성하여 Peer 로 broadcast 한다.
    Peer 의 BlockManager 는 전달 받은 Block 을 검증 처리 한다.
    """
    def __init__(self, common_service, peer_id, channel_name, level_db_identity):
        self.__channel_name = channel_name
        self.__level_db = None
        self.__level_db_path = ""
        self.__init_level_db(f"{level_db_identity}_{channel_name}")
        self.__peer_id = peer_id if peer_id is not None else self.__make_peer_id()
        self.__txQueue = queue.Queue()
        self.__unconfirmedBlockQueue = queue.Queue()
        self.__candidate_blocks = CandidateBlocks(self.__peer_id, channel_name)
        self.__common_service = common_service
        self.__blockchain = BlockChain(self.__level_db, channel_name)
        self.__total_tx = self.__blockchain.rebuild_blocks()
        self.__peer_type = None
        self.__block_type = BlockType.general
        self.__consensus = None
        self.__run_logic = None
        self.set_peer_type(loopchain_pb2.PEER)

    @property
    def channel_name(self):
        return self.__channel_name

    @property
    def peer_type(self):
        return self.__peer_type

    @property
    def consensus(self):
        return self.__consensus

    @property
    def block_type(self):
        return self.__block_type

    @block_type.setter
    def block_type(self, block_type):
        self.__block_type = block_type

    def __init_level_db(self, level_db_identity):
        """init Level Db

        :param level_db_identity: identity for leveldb
        :return:
        """
        level_db = None

        db_default_path = osp.join(conf.DEFAULT_STORAGE_PATH, 'db_' + level_db_identity)
        db_path = db_default_path

        retry_count = 0
        while level_db is None and retry_count < conf.MAX_RETRY_CREATE_DB:
            try:
                level_db = leveldb.LevelDB(db_path, create_if_missing=True)
            except leveldb.LevelDBError:
                db_path = db_default_path + str(retry_count)
            retry_count += 1

        if level_db is None:
            logging.error("Fail! Create LevelDB")
            raise leveldb.LevelDBError("Fail To Create Level DB(path): " + db_path)

        self.__level_db = level_db
        self.__level_db_path = db_path

    def __make_peer_id(self):
        """네트워크에서 Peer 를 식별하기 위한 UUID를 level db 에 생성한다.
        """
        if self.__channel_name != conf.LOOPCHAIN_DEFAULT_CHANNEL:
            util.exit_and_msg(f"Only default channel can make peer id!")

        try:
            uuid_bytes = bytes(self.__level_db.Get(conf.LEVEL_DB_KEY_FOR_PEER_ID))
            peer_id = uuid.UUID(bytes=uuid_bytes)
        except KeyError:  # It's first Run
            peer_id = None

        if peer_id is None:
            peer_id = uuid.uuid1()
            logging.info("make new peer_id: " + str(peer_id))
            self.__level_db.Put(conf.LEVEL_DB_KEY_FOR_PEER_ID, peer_id.bytes)

        return str(peer_id)

    def get_peer_id(self):
        return self.__peer_id

    def get_level_db(self):
        return self.__level_db

    def clear_all_blocks(self):
        logging.debug(f"clear level db({self.__level_db_path})")
        shutil.rmtree(self.__level_db_path)

    def set_peer_type(self, peer_type):
        self.__peer_type = peer_type

        if self.__peer_type == loopchain_pb2.BLOCK_GENERATOR:
            if conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.none:
                self.__consensus = ConsensusNone(self)
            elif conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.siever:
                self.__consensus = ConsensusSiever(self)
            elif conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.lft:
                self.__consensus = ConsensusLFT(self)
            else:
                self.__consensus = ConsensusDefault(self)
            self.__run_logic = self.__consensus.consensus
        else:
            self.__run_logic = self.__do_vote

    def get_total_tx(self):
        """
        블럭체인의 Transaction total 리턴합니다.

        :return: 블럭체인안의 transaction total count
        """
        return self.__total_tx

    def get_blockchain(self):
        return self.__blockchain

    def get_candidate_blocks(self):
        return self.__candidate_blocks

    def broadcast_getstatus(self):
        """peer 들의 접속 상태를 확인하기 위해서 status 조회를 broadcast 로 모든 peer 에 전달한다.
        """
        logging.info("BroadCast GetStatus....")
        if self.__common_service is not None:
            self.__common_service.broadcast("GetStatus",
                                            (loopchain_pb2.StatusRequest(request="BlockGenerator BroadCast")))

    def broadcast_send_unconfirmed_block(self, block):
        """생성된 unconfirmed block 을 피어들에게 broadcast 하여 검증을 요청한다.
        """
        logging.debug("BroadCast AnnounceUnconfirmedBlock...peers: " +
                      str(ObjectManager().peer_service.channel_manager.get_peer_manager(
                          self.__channel_name).get_peer_count()))

        dump = pickle.dumps(block)
        if len(block.confirmed_transaction_list) > 0:
            self.__blockchain.increase_made_block_count()
        if self.__common_service is not None:
            self.__common_service.broadcast("AnnounceUnconfirmedBlock",
                                            (loopchain_pb2.BlockSend(
                                                block=dump,
                                                channel=self.__channel_name)))

    def broadcast_announce_confirmed_block(self, block_hash, block=None):
        """검증된 block 을 전체 peer 에 announce 한다.
        """
        logging.info("BroadCast AnnounceConfirmedBlock....")
        if self.__common_service is not None:
            if block is not None:
                dump = pickle.dumps(block)
                self.__common_service.broadcast("AnnounceConfirmedBlock",
                                                (loopchain_pb2.BlockAnnounce(
                                                    block_hash=block_hash,
                                                    channel=self.__channel_name,
                                                    block=dump)))
            else:
                self.__common_service.broadcast("AnnounceConfirmedBlock",
                                                (loopchain_pb2.BlockAnnounce(
                                                    block_hash=block_hash,
                                                    channel=self.__channel_name)))

    def broadcast_audience_set(self):
        """Check Broadcast Audience and Return Status

        """
        self.__common_service.broadcast_audience_set()

    def add_tx(self, tx):
        """전송 받은 tx 를 Block 생성을 위해서 큐에 입력한다. txQueue 는 unloaded(dump) object 를 처리하므로
        tx object 는 dumps 하여 입력한다.

        :param tx: transaction object
        """
        tx_unloaded = pickle.dumps(tx)
        self.__txQueue.put(tx_unloaded)

    def add_tx_unloaded(self, tx):
        """전송 받은 tx 를 Block 생성을 위해서 큐에 입력한다. load 하지 않은 채 입력한다.

        :param tx: transaction object
        """
        self.__txQueue.put(tx)

    def get_tx(self, tx_hash):
        """tx_hash 로 저장된 tx 를 구한다.

        :param tx_hash: 찾으려는 tx 의 hash
        :return: tx object or None
        """
        return self.__blockchain.find_tx_by_key(tx_hash)

    def get_invoke_result(self, tx_hash):
        """ get invoke result by tx

        :param tx_hash:
        :return:
        """
        return self.__blockchain.find_invoke_result_by_tx_hash(tx_hash)

    def get_tx_queue(self):
        return self.__txQueue

    def get_count_of_unconfirmed_tx(self):
        """BlockManager 의 상태를 확인하기 위하여 현재 입력된 unconfirmed_tx 의 카운트를 구한다.

        :return: 현재 입력된 unconfirmed tx 의 갯수
        """
        return self.__txQueue.qsize()

    def confirm_block(self, block_hash):
        try:
            self.__total_tx += self.__blockchain.confirm_block(block_hash)
        except BlockchainError as e:
            logging.warning("BlockchainError, retry block_height_sync")
            ObjectManager().peer_service.block_height_sync()

    def add_unconfirmed_block(self, unconfirmed_block):
        # siever 인 경우 블럭에 담긴 투표 결과를 이전 블럭에 반영한다.
        if conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.siever:
            if unconfirmed_block.prev_block_confirm:
                # logging.debug(f"block confirm by siever: "
                #               f"hash({unconfirmed_block.prev_block_hash}) "
                #               f"block.channel({unconfirmed_block.channel_name})")

                self.confirm_block(unconfirmed_block.prev_block_hash)
            elif unconfirmed_block.block_type is BlockType.peer_list:
                logging.debug(f"peer manager block confirm by siever: "
                              f"hash({unconfirmed_block.block_hash}) block.channel({unconfirmed_block.channel_name})")
                self.confirm_block(unconfirmed_block.block_hash)
            else:
                # 투표에 실패한 블럭을 받은 경우
                # 특별한 처리가 필요 없다. 새로 받은 블럭을 아래 로직에서 add_unconfirm_block 으로 수행하면 된다.
                pass
        elif conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.lft:
            if unconfirmed_block.prev_block_confirm:

                # turn off previous vote's timer when a general peer received new block for vote
                ObjectManager().peer_service.timer_service.stop_timer(unconfirmed_block.prev_block_hash)
                # logging.debug(f"block confirm by lft: "
                #               f"hash({unconfirmed_block.prev_block_hash}) "
                #               f"block.channel({unconfirmed_block.channel_name})")

                self.confirm_block(unconfirmed_block.prev_block_hash)
            elif unconfirmed_block.block_type is BlockType.peer_list:
                logging.debug(f"peer manager block confirm by lft: "
                              f"hash({unconfirmed_block.block_hash}) block.channel({unconfirmed_block.channel_name})")
                self.confirm_block(unconfirmed_block.block_hash)
            else:
                # 투표에 실패한 블럭을 받은 경우
                # 특별한 처리가 필요 없다. 새로 받은 블럭을 아래 로직에서 add_unconfirm_block 으로 수행하면 된다.
                pass

        self.__unconfirmedBlockQueue.put(unconfirmed_block)

    def add_block(self, block):
        self.__total_tx += block.confirmed_transaction_list.__len__()
        self.__blockchain.add_block(block)

    def run(self):
        """Block Manager Thread Loop
        PEER 의 type 에 따라 Block Generator 또는 Peer 로 동작한다.
        Block Generator 인 경우 conf 에 따라 사용할 Consensus 알고리즘이 변경된다.
        """

        logging.info(f"channel({self.__channel_name}) Block Manager thread Start.")

        while self.is_run():
            self.__run_logic()

        logging.info(f"channel({self.__channel_name}) Block Manager thread Ended.")

    def __do_vote(self):
        """Announce 받은 unconfirmed block 에 투표를 한다.
        """
        if not self.__unconfirmedBlockQueue.empty():
            unconfirmed_block = self.__unconfirmedBlockQueue.get()
            logging.debug("we got unconfirmed block ....")
        else:
            time.sleep(conf.SLEEP_SECONDS_IN_SERVICE_LOOP)
            # logging.debug("No unconfirmed block ....")
            return

        logging.info("PeerService received unconfirmed block: " + unconfirmed_block.block_hash)

        if unconfirmed_block.confirmed_transaction_list.__len__() == 0 and \
                unconfirmed_block.block_type is not BlockType.peer_list:
            # siever 에서 사용하는 vote block 은 tx 가 없다. (검증 및 투표 불필요)
            # siever 에서 vote 블럭 발송 빈도를 보기 위해 warning 으로 로그 남김, 그 외의 경우 아래 로그는 주석처리 할 것
            # logging.warning("This is vote block by siever")
            pass
        else:
            # block 검증
            block_is_validated = False
            try:
                block_is_validated = Block.validate(unconfirmed_block, self.__txQueue)
            except Exception as e:
                logging.error(e)

            if block_is_validated:
                # broadcast 를 받으면 받은 블럭을 검증한 후 검증되면 자신의 blockchain 의 unconfirmed block 으로 등록해 둔다.
                confirmed, reason = self.__blockchain.add_unconfirm_block(unconfirmed_block)
                if confirmed:
                    # block is confirmed
                    # validated 일 때 투표 할 것이냐? confirmed 일 때 투표할 것이냐? 현재는 validate 만 체크
                    pass
                elif reason == "block_height":
                    # Announce 되는 블럭과 자신의 height 가 다르면 Block Height Sync 를 다시 시도한다.

                    ObjectManager().peer_service.block_height_sync()

            self.__common_service.vote_unconfirmed_block(
                unconfirmed_block.block_hash, block_is_validated, self.__channel_name)

            if conf.CONSENSUS_ALGORITHM == conf.ConsensusAlgorithm.lft:
                # turn on timer when peer type is general after vote
                # TODO: set appropriate callback function and parameters
                timer = Timer(
                    unconfirmed_block.block_hash,
                    conf.TIMEOUT_FOR_PEER_VOTE,
                    ObjectManager().peer_service.timer_test_callback_function,
                    ["test after vote by block_manager"]
                )
                ObjectManager().peer_service.timer_service.add_timer(unconfirmed_block.block_hash, timer)
