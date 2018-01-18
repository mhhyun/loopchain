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
"""loopchain timer service."""

from loopchain.baseservice import CommonThread
from loopchain.blockchain import *
from enum import Enum


class OffType(Enum):
    """enum class of reason to turn off timer"""

    normal = 0
    time_out = 1


class Timer:
    """timer object"""

    def __init__(self, target, duration, callback=None, args=None):
        """initial function

        :param target:      target of timer
        :param duration:    duration for checking timeout
        :param callback:    callback function after timeout or normal case
        :param args:        parameters for callback function
        """
        # logging.debug(f'--- set timer info :{duration}')
        self.__target = target
        self.__duration = duration
        self.__start_time = time.time()
        self.__callback = callback
        self.__kwargs = {}

        if args is None:
            self.__args = []
        else:
            self.__args = args

    @property
    def target(self):
        return self.__target

    def is_timeout(self):
        if time.time() - self.__start_time < self.__duration:
            return False

        logging.debug(f'gab:{time.time() - self.__start_time}')
        return True

    def on(self):
        pass
        logging.debug('timer is on')

    def off(self, off_type):
        """turn off timer by type

        :param off_type: type of reason to turn off timer
        """
        if off_type is OffType.time_out:
            logging.debug('timer is turned off by timeout')
            self.__callback(*self.__args, **self.__kwargs)


class TimerService(CommonThread):
    """timer service"""

    def __init__(self):
        CommonThread.__init__(self)
        self.__timer_list = {}

    @property
    def timer_list(self):
        return self.__timer_list

    def add_timer(self, key, timer):
        """add timer to self.__timer_list

        :param key: key
        :param timer: timer object
        :return:
        """
        self.__timer_list[key] = timer
        timer.on()
        # logging.debug(f'+++++++++++++++++++add timer')
        # logging.debug(f'--- list : {self.__timer_list}')

    def remove_timer(self, key):
        """remove timer from self.__timer_list

        :param key: key
        :return:
        """
        if key in self.__timer_list:
            del self.__timer_list[key]
            # logging.debug(f'----------------remove timer')
            # logging.debug(f'--- list : {self.__timer_list}')
        else:
            logging.warning(f'({key}) is not in timer list.')

    def get_timer(self, key):
        """get a timer by key

        :param key: key
        :return: a timer by key
        """
        if key in self.__timer_list.keys():
            return self.__timer_list[key]
        else:
            logging.warning(f'There is no value by this key: {key}')
            return None

    def stop_timer(self, key, off_type=OffType.normal):
        """stop timer

        :param key: key
        :param off_type: type of reason to turn off timer
        :return:
        """
        if key in list(self.__timer_list):
            self.__timer_list[key].off(off_type)
            self.remove_timer(key)
        else:
            logging.warning(f'There is no value by this key: {key}')
            return None

    def run(self):
        while self.is_run():
            time.sleep(1)
            # logging.debug(f'--- start leader complain timer service...')
            # logging.debug(f'--- list : {self.__timer_list}')

            for key in list(self.__timer_list):
                timer = self.__timer_list[key]

                if timer.is_timeout():
                    self.stop_timer(key, OffType.time_out)
                else:
                    pass
