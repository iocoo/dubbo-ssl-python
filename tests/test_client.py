# -*- coding: utf-8 -*-
import json
import threading
import pytest
from unittest.mock import patch 

from dubbo_ssl.client import DubboClient
from dubbo_ssl.client import ZkRegister
from dubbo_ssl.codec.encoder import Object
from dubbo_ssl.common.exceptions import (RegisterException)

class TestDubboClient:

    def test_init_without_host_and_zk_raises(self):
        with pytest.raises(RegisterException):
            DubboClient(interface="com.example.Svc",group="default")
    
    def test_init_with_host(self):
        client = DubboClient(interface="com.example.Svc",group="default",
                             host="127.0.0.1:20880",verify=None)
        assert client is not None

    def test_call_auto_wraps_single_arg(self):
        client = DubboClient(interface="com.example.Svc",group="default",
                             host="127.0.0.1:20880",verify=None)
        with patch("dubbo_ssl.client.connection_pool") as mock_pool:
            mock_pool.get.return_value = "result"
            client.call(method="process",args="single_arg")
            call_args = mock_pool.get.call_args
            request_param = call_args[0][1]
            assert request_param["arguments"] == ["single_arg"]




