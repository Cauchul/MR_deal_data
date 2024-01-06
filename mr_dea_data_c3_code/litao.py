# -*- coding: utf-8 -*-

import json
import os
import sys
import pytest
from kafka import KafkaProducer


class TestBackendServices:
    def setup_class(self):
        self.bootstrap_servers = '172.16.23.180:9092'
        self.topic = 'm_pkx_ue_mr_02'
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def test_send_uemrdata_to_4g_topic(self):
        fiveg_uemr_file = r"C:\Users\Administrator\Desktop\5guemr23.txt"
        i = 0
        with open(fiveg_uemr_file, "rb") as f:
            for line in f:
                i = i+1
                print("发送第{0}条 5G UEMR数据:{1}到主题:{2}" .format(i, line.strip(), self.topic))
                self.producer.send(self.topic, line.strip())
                self.producer.flush()

    def teardown_class(self):
        self.producer.close()


if __name__ == "main":
    pytest.main([__file__])