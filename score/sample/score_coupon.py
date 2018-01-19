#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from os.path import dirname
import uuid
import datetime
import logging
from enum import Enum

from loopchain.blockchain import ScoreBase
from loopchain.tools import ScoreDatabaseType, ScoreHelper


class CouponMethod(Enum):
    """
        쿠폰 스코어 메서드 이름
    """
    # Invoke Methods
    CREATE = "create"
    BUY = "buy"
    USE = "use"
    SEND = "send"

    # Query Methods
    GET_COUPON_INFO = "get_coupon_info"


class CouponStatus(Enum):
    """ 쿠폰상태 열거형

    """
    CREATED = "created"
    SOLD = "sold"
    USED = "used"
    EXPIRED = "expired"
    CALCULATED = "calculated"


class CouponLogFmt(Enum):
    """로그메시지 포맷

    """
    INFO = "[{type}] {method} : {result} "
    ERROR = "[{type}] {method} : {error}"


class UserScore(ScoreBase):
    """
        심플 쿠폰-스코어 객체
    """
    __db_id = 'coupon.db'
    __db = None
    __score_helper = None

    def __init__(self, info=None):
        """생성자
        """
        super().__init__(info)

        self.__methods = {
            CouponMethod.CREATE.value: self.create,
            CouponMethod.BUY.value: self.buy,
            CouponMethod.USE.value: self.use,
            CouponMethod.SEND.value: self.send,
            CouponMethod.GET_COUPON_INFO.value: self.get_coupon_info
        }
        self.__init_score_info(info)
        self.__init_score_helper()
        self.__init_db()

    def __init_score_info(self, info):
        """ Read package.json file as SCORE package information.

        ScoreHelper is special module to capsulize SCORE operation.
        """
        try:
            if info is None:
                with open(dirname(__file__)+'/'+ScoreBase.PACKAGE_FILE, 'r') as f:
                    self.__score_info = json.loads(f.read())
                    f.close()
            else:
                self.__score_info = info
            logging.info(CouponLogFmt.INFO.value.format(type='Init', method='__init_score_info', result='OK'))
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Init', method='__init_score_info', error=e))

    def __init_score_helper(self):
        """ Initialize ScoreHelper().

        ScoreHelper is special module to capsulize SCORE operation.
        """
        try:
            if self.__score_helper is None:
                self.__score_helper = ScoreHelper()
            logging.info(CouponLogFmt.INFO.value.format(type='Init', method='__init_score_helper', result='OK'))
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Init', method='__init_score_helper', error=e))

    def __init_db(self):
        """ Initialize database for SCORE

        SCORE have to store all data into its own database.
        """
        try:
            if self.__db is None:
                self.__db = LocalDB(self.__db_id)
            logging.info(CouponLogFmt.INFO.value.format(type='Init', method='__init_db', result='OK db:%s' % self.__db_id))
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Init', method='__init_db', error=e))

    def info(self):
        pass

    def invoke(self, transaction, block):
        """coupon score data 생성 또는 갱신
        :param transaction:
        :param block:
        :return:
        """
        try:

            data = transaction.get_data_string()
            tx_data = json.loads(data)
            logging.info(CouponLogFmt.INFO.value.format(type='Invoke', method='invoke', result='tx_data:'+ str(tx_data)))
            return self.__methods[tx_data['method']](tx_data['params'])
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Invoke', method='invoke', error=e))

    def query(self, request):
        """ coupon score data 조회
        :param request:
        :return:
        """
        try:
            request = json.loads(request)
            method_name = request['method']
            method_params = request['params']
            query_data = self.__methods[method_name](method_params)

            if query_data is not None:
                response = {"jsonrpc": "2.0", "code": 0, "response": {"coupon_count": len(query_data), "coupon_list": query_data}}
            else:
                response = {"jsonrpc": "2.0", "code": -1, "response": {}, }

            return json.dumps(response)

        except Exception as e:
            logging.error("query error" + str(e))

    def create(self, params) -> dict:
        """ 쿠폰신규발행
        :return: 쿠폰 객체
        """
        try:
            coupon = {
                "coupon_code": str(uuid.uuid4()),
                "owner": None,
                "expired_at": (datetime.datetime.now() + datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S'),
                "created_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "sold_at": None,
                "status": CouponStatus.CREATED.value
            }
            self.__db.put(coupon["coupon_code"], coupon)
            logging.info(CouponLogFmt.INFO.value.format(type='Invoke', method='create', result=json.dumps(coupon)))

            result = {
                "code": 0,       # 'code' 키는 결과값에 대한 필수 예약 키로 사용된다.
                "data": coupon,
                "more_info": coupon
            }

            return result
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Invoke', method='create', error=e))

    def buy(self, params) -> dict:
        """ 쿠폰구입
        :param params:
        :return: 구입한 쿠폰객체
        """
        try:
            coupon = None
            it = self.__db.iterator()
            for key, val in it:
                val = json.loads(val)
                if val['status'] is not None and val.get('status') == CouponStatus.CREATED.value:
                    val['owner'] = params['owner']
                    val['sold_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    val['status'] = CouponStatus.SOLD.value
                    coupon = val
                    break

            self.__db.put(coupon.get('coupon_code'), coupon)
            logging.info(CouponLogFmt.INFO.value.format(type='Invoke', method='buy', result=coupon))

            result = {
                "code": 0,  # 'code' 키는 결과값에 대한 필수 예약 키로 사용된다.
                "data": coupon
            }

            return result
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Invoke', method='buy', error=e))

    def use(self, params) -> dict:
        try:
            coupon = self.__db.get(params['coupon_code'])
            if params.get('owner') == coupon.get('owner'):
                coupon['status'] = CouponStatus.USED.value
                coupon['used_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.__db.put(coupon.get('coupon_code'), coupon)
                logging.info(CouponLogFmt.INFO.value.format(type='Invoke', method='use', result=coupon))

                result = {
                    "code": 0,  # 'code' 키는 결과값에 대한 필수 예약 키로 사용된다.
                    "data": coupon
                }

            return result
            # TODO:쿠폰소유자가 아닌 예외처리
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Invoke', method='use', error=e))

    def send(self, params) -> dict:
        try:
            coupon = self.__db.get(params['coupon_code'])
            if params.get('owner') == coupon.get('owner'):
                coupon['owner'] = params['to']
                coupon['from'] = params['owner']
                coupon['sent_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.__db.put(coupon.get('coupon_code'), coupon)
                logging.info(CouponLogFmt.INFO.value.format(type='Invoke', method='send', result=coupon))

                result = {
                    "code": 0,  # 'code' 키는 결과값에 대한 필수 예약 키로 사용된다.
                    "data": coupon
                }

            return result
            # TODO:쿠폰소유자가 아닌 예외처리
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Invoke', method='send', error=e))

    def get_coupon_info(self, params) -> list:
        """ get coupon info
        :param params: coupon_code in json_params
        :return:
        """
        coupon_list = []
        try:
            # coupon_code(key)로만 직접 또는 여러 조건으로 조회를 구분
            if 'coupon_code' in params.keys() and len(params) == 1:
                coupon_code = params['coupon_code']
                coupon = self.__db.get(coupon_code)
                coupon_list.append(coupon)
                # logging.info(CouponLogFmt.INFO.value.format(type='Query', method='get_coupon_info', result=coupon))
            else:
                coupon_list = []
                filters = params.items()
                it = self.__db.iterator()

                for key, val in it:
                    val = json.loads(val)
                    if len(filters - val.items()) == 0:
                        coupon_list.append(val)
            logging.info(CouponLogFmt.INFO.value.format(type='Query', method='get_coupon_info', result=coupon_list))

            return coupon_list
        except Exception as e:
            logging.error(CouponLogFmt.ERROR.value.format(type='Query', method='get_coupon_info', error=e))


class LocalDB:

    DB_ENCODING = "utf-8"

    def __init__(self, db_name):
        helper = ScoreHelper()
        self.db = helper.load_database(score_id=db_name, database_type=ScoreDatabaseType.leveldb)

    def get(self, key):
        """
        :param key: string key
        :return:
        """
        byte_key = bytes(str(key), self.DB_ENCODING)
        try:
            return json.loads(self.db.Get(byte_key), encoding=self.DB_ENCODING)
        except Exception as e:
            raise e

    def put(self, key, value):
        """
        :param key: string key
        :param value: string value
        :return:
        """

        if(isinstance(value, dict)) is True:
            value = json.dumps(value)

        byte_key = bytes(str(key), self.DB_ENCODING)
        byte_value = bytes(str(value), self.DB_ENCODING)

        self.db.Put(byte_key, byte_value)

    def iterator(self):
        return self.db.RangeIter()
