#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
"""Test ScoreCoupon functions"""

import sys
import unittest
from os.path import dirname, abspath
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))) + '/score/sample')
from score_coupon import *


class TestCoupon(unittest.TestCase):
    score_coupon = UserScore()
    coupon = None

    def setUp(self):
        pass

    def tearDown(self):
        if self.coupon is not None:
            self.assertTrue(self.coupon.get('coupon_code'), "쿠폰의 코드가 없음")
            query_params = '{"method": "get_coupon_info", "params": {"coupon_code": "%s"}}' % self.coupon['coupon_code']
            self.score_coupon.query(query_params)

            self.__class__.coupon = self.coupon

    def test_0_create(self):
        result = self.score_coupon.create({})
        self.coupon = result.get('data')

        self.assertIsNotNone(self.coupon, "신규 발행 쿠폰 정보 없음")
        self.assertIsNotNone(self.coupon.get('created_at'), "쿠폰 발행일시가 있어야 한다.")
        self.assertEqual(CouponStatus(self.coupon.get('status')), CouponStatus.CREATED, "발행된 쿠폰의 상태는 'created'이어야 한다.")

    def test_1_buy(self):
        buy_params = {"owner": "wise"}
        result = self.score_coupon.buy(buy_params)
        self.coupon = result.get('data')
        self.coupon_code = self.coupon.get('coupon_code')

        self.assertIsNotNone(self.coupon, "구매 쿠폰 정보 없음")
        self.assertEqual(self.coupon.get('owner'), buy_params.get('owner'), "쿠폰 소유자가 구매자와 동일해야 한다.")
        self.assertIsNotNone(self.coupon.get('sold_at'), "쿠폰 구매일시가 있어야 한다.")
        self.assertEqual(CouponStatus(self.coupon.get('status')), CouponStatus.SOLD, "구입한 쿠폰의 상태는 'sold'이어야 한다.")

    def test_2_send(self):
        self.coupon = self.__class__.coupon

        send_params = {"coupon_code": self.coupon['coupon_code'], "owner": "wise", "to": "yuju"}
        result = self.score_coupon.send(send_params)
        self.coupon = result.get('data')

        self.assertIsNotNone(self.coupon.get('sent_at'), "쿠폰 선물일시가 있어야 한다.")
        self.assertEqual(self.coupon.get('from'), send_params.get('owner'), "쿠폰의 from이 선물 보낸사람으로 변경되어야 한다.")
        self.assertEqual(self.coupon.get('owner'), send_params.get('to'), "쿠폰의 onwer가 선물 받을사람으로 owner가 변경되어야 한다.")

    def test_3_use(self):
        self.coupon = self.__class__.coupon

        use_params = {"coupon_code": self.coupon.get('coupon_code'), "owner": self.coupon.get('owner')}
        result = self.score_coupon.use(use_params)
        self.coupon = result.get('data')

        self.assertIsNotNone(self.coupon.get('used_at'), "쿠폰 사용일시가 있어야 한다.")
        self.assertEqual(CouponStatus(self.coupon.get('status')), CouponStatus.USED, "구입한 쿠폰의 상태는 'used'이어야 한다.")

if __name__ == '__main__':
    unittest.main()
