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
""" A class for authorization of Peer """

import datetime
import logging
import binascii

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding, ec, utils

import loopchain.utils as util
from loopchain.tools import CertVerifier


class PeerAuthorization(CertVerifier):
    """ Peer의 인증을 처리한다 """
    # 인증서/개인키 파일명
    CERT_FILE = "cert.pem"
    PRI_FILE = "key.pem"

    # Peer 보관 CA 인증서 파일명
    PEER_CA_CERT_NAME = "ca.der"

    __peer_pri = None
    __ca_cert = None
    __token = None

    # RequestPeer 요청 생성 시 저장 정보
    __peer_info = None

    def __init__(self, cert_file, pri_file, cert_pass):
        logging.debug(f"cert file : {cert_file}")
        logging.debug(f"private file : {pri_file}")
        with open(cert_file, "rb") as der:
            cert_bytes = der.read()
            super().__init__(cert_bytes)

        self.__load_private(pri_file, cert_pass)

    def __load_private(self, pri_file, cert_pass):
        """인증서 로드

        :param pri_file: 개인키 경로
        :param cert_pass: 개인키 패스워드
        :return:
        """
        # TODO KMS 정책 정해지면 살리기
        # ca_cert_file = join(cert_path, self.PEER_CA_CERT_NAME)

        # 인증서/개인키 로드
        with open(pri_file, "rb") as der:
            private_bytes = der.read()
            try:
                self.__peer_pri = serialization.load_pem_private_key(private_bytes, cert_pass, default_backend())
            except ValueError:
                util.exit_and_msg("Invalid Password")

        # TODO KMS 정책 정해지면 살리기
        # CA 인증서 로드
        # with open(ca_cert_file, "rb") as der:
        #     cert_bytes = der.read()
        #     self.__ca_cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())

        # 키 쌍 검증
        sign = self.sign_data(b'TEST')
        if self.verify_data(b'TEST', sign) is False:
            util.exit_and_msg("Invalid Signature(Peer Certificate load test)")

    def set_peer_info(self, peer_id, peer_target, group_id, peer_type):
        self.__peer_info = b''.join([peer_id.encode('utf-8'),
                                     peer_target.encode('utf-8'),
                                     group_id.encode('utf-8')]) + bytes([peer_type])

    def sign_data(self, data, is_hash=False):
        """인증서 개인키로 DATA 서명

        :param data: 서명 대상 원문
        :param is_hash: when data is hashed True
        :return: 서명 데이터
        """
        hash_algorithm = hashes.SHA256()
        if is_hash:
            hash_algorithm = utils.Prehashed(hash_algorithm)
            if isinstance(data, str):
                try:
                    data = binascii.unhexlify(data)
                except Exception as e:
                    logging.error(f"hash data must hex string or bytes \n exception : {e}")
                    return None

        if not isinstance(data, (bytes, bytearray)):
            logging.error(f"data must be bytes \n")
            return None

        if isinstance(self.__peer_pri, ec.EllipticCurvePrivateKeyWithSerialization):
            return self.__peer_pri.sign(
                data,
                ec.ECDSA(hash_algorithm))
        elif isinstance(self.__peer_pri, rsa.RSAPrivateKeyWithSerialization):
            return self.__peer_pri.sign(
                data,
                padding.PKCS1v15(),
                hash_algorithm
            )
        else:
            logging.error("Unknown PrivateKey Type : %s", type(self.__peer_pri))
            return None

    def generate_request_sign(self, rand_key):
        """RequestPeer 서명을 생성한다.

        set_peer_info 함수가 우선 실행되어야 한다.
        sign_peer(peer_id || peer_target || group_id || peet_type || rand_key)
        :param rand_key: 서버로 부터 수신한 랜덤
        :return: 서명
        """
        tbs_data = self.__peer_info + bytes.fromhex(rand_key)
        return self.sign_data(tbs_data)

    def get_token_time(self, token):
        """Token의 유효시간을 검증하고 토큰을 검증하기 위한 데이터를 반환한다.

        :param token: 검증 대상 Token
        :return: 검증 실패 시 None, 성공 시 토큰 검증을 위한 데이터
        """
        token_time = token[2:18]
        token_date = int(token_time, 16)
        current_date = int(datetime.datetime.now().timestamp() * 1000)
        if current_date < token_date:
            return bytes.fromhex(token_time)

        return None
