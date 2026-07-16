# -*- coding: utf-8 -*-

import logging
import random
import threading
import time
from urllib.parse import quote

from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState

from dubbo_ssl.common.constants import DUBBO_ZK_PROVIDERS, DUBBO_ZK_CONFIGURATORS, DUBBO_ZK_CONSUMERS
from dubbo_ssl.common.exceptions import RegisterException
from dubbo_ssl.common.util import parse_url, get_ip, get_pid
from dubbo_ssl.connection.connections import connection_pool

logger = logging.getLogger('dubbo_ssl')


class DubboClient(object):
    """
    用于实现 dubbo 调用的客户端
    """

    def __init__(self, interface, group, version='1.0.0', dubbo_version='2.4.10', zk_register=None, host=None, verify=None):
        """
        :param interface: 接口名，例如：com.qianmi.pc.es.api.EsProductQueryProvider
        :param version: 接口的版本号，例如：1.0.0，默认为1.0.0
        :param dubbo_version: dubbo的版本号，默认为2.4.10
        :param zk_register: zookeeper注册中心管理端，参见类：ZkRegister
        :param host: 远程主机地址，用于绕过zookeeper进行直连，例如：172.21.4.98:20882
        :param verify:[None|True|False] None-> non-TLS True -> 启用TLS，验证 dubbo SSL/TLS证书  False-> 启用TLS 不验证服务端证书
        """
        if not zk_register and not host:
            raise RegisterException('zk_register和host至少需要填入一个')

        logger.debug('Created client, interface={}, version={}, group={}'.format(interface, version, group))

        self.__interface = interface
        self.__group = group
        self.__version = version
        self.__dubbo_version = dubbo_version


        self.__zk_register = zk_register
        self.__host = host
        self.__verify = verify

    def call(self, method, args=(), context=None, timeout=None):

        if not isinstance(args, (list, tuple)):
            args = [args]

        if self.__zk_register:
            host = self.__zk_register.get_provider_host(self.__interface, self.__version, self.__group)
        else:
            host = self.__host

        verify = self.__verify

        request_param = {
            'dubbo_version': self.__dubbo_version,
            'version': self.__version,
            'method': method,
            'path': self.__interface,
            'arguments': args,
            'group': self.__group,
            'context': context
        }

        logger.debug('Start request, host={}, params={}'.format(host, request_param))
        start_time = time.time()
        result = connection_pool.get(host, request_param, timeout, verify)
        cost_time = int((time.time() - start_time) * 1000)
        logger.debug('Finish request, host={}, params={}'.format(host, request_param))
        logger.debug('Request invoked, host={}, params={}, result={}, cost={}ms, timeout={}s'.format(
            host, request_param, result, cost_time, timeout))
        return result


class ZkRegister(object):

    def __init__(self, hosts, verify_certs=False, use_ssl=False,
                 ca_bundle=None, client_cert=None, client_cert_key=None,
                 application_name='ConsumerApp'):
        self.hosts = {}
        # watch callback
        self._raw_providers = {}
        self._subscribed_interfaces = set()
        self._interface_lock = threading.Lock()
        zk = KazooClient(hosts=hosts,verify_certs=verify_certs,ca=ca_bundle,
                         certfile=client_cert,keyfile=client_cert_key,use_ssl=use_ssl)
        zk.add_listener(self.state_listener)
        zk.start()

        self.zk = zk
        self.application_name = application_name
        self.weights = {}
        self.lock = threading.Lock()

    def state_listener(self, state):
        """
        监听应用和Zookeeper之间的连接状态
        :param state:
        :return:
        """
        logger.debug('Current state -> {}'.format(state))
        if state == KazooState.LOST:
            logger.debug('The session to register has lost.')
        elif state == KazooState.SUSPENDED:
            logger.debug('Disconnected from zookeeper.')
        else:
            logger.debug('Connected or disconnected to zookeeper.')

            # 在新的线程里面进行重新订阅以防止死锁
            t = threading.Thread(target=self.__resubscribe)
            t.start()

    def __resubscribe(self):
        """
        由于与Zookeeper的连接断开，所以需要重新订阅消息
        :return:
        """
        for interface in list(self._subscribed_interfaces):
            self.zk.get_children(DUBBO_ZK_PROVIDERS.format(interface), watch=self._watch_children)
            self.zk.get_children(DUBBO_ZK_CONFIGURATORS.format(interface), watch=self._watch_configurators)

    def _watch_children(self, event):
        """
        对某个provider下的子节点进行监听，一旦provider发生了变化则对本地缓存进行更新
        :param event:
        :return:
        """
        path = event.path
        logger.debug('zookeeper path: {} changed'.format(path))

        interface = path.split('/')[2]

        providers = self.zk.get_children(path, watch=self._watch_children)
        dubbo_providers = list(filter(lambda p: p['scheme'] == 'dubbo', map(parse_url, providers))) 
        if not dubbo_providers:
            logger.debug('no providers found for {}'.format(interface))
            self._raw_providers[interface] = []
            self._clear_hosts_for_interface(interface)
            return
        
        self._raw_providers[interface] = dubbo_providers
        self._refresh_host_cache(interface)
        logger.debug('interface: {} raw_providers updated,count:{}'.format(interface, len(dubbo_providers)))

    def _clear_hosts_for_interface(self,interface):
        """清空指定interface 下的缓存"""
        keys_to_remove = [k for k in self.hosts if k[0] == interface]
        for key in keys_to_remove:
            self.hosts[key] = []

    def _refresh_host_cache(self,interface):
        """按照group+version重新过滤更新已注册的缓存"""
        dubbo_providers = self._raw_providers.get(interface, [])
        subscribed_keys = [k for k in self.hosts if k[0] == interface]
        for key in subscribed_keys:
            _, version, group = key
            target_providers = [
                p for p in dubbo_providers if p.get('fields',{}).get('version') == version 
                and p.get('fields',{}).get('group') == group
            ]
            self.hosts[key] = [p['host'] for p in target_providers]
            logger.debug('refreshed cache for key:{},hosts:{}'.format(key, self.hosts[key]))


    def get_provider_host(self, interface, version, group):
        """
        从zk中可以根据接口名称获取到此接口某个provider的host
        :param interface: dubbo interface name
        :param version: 
        :param group: 
        :return:
        """
        cache_key = (interface, version, group)
        if cache_key not in self.hosts:
            self.lock.acquire()
            try:
                if cache_key not in self.hosts:
                    path = DUBBO_ZK_PROVIDERS.format(interface)
                    if self.zk.exists(path):
                        self._get_providers_from_zk(path, interface, version, group)
                        self._get_configurators_from_zk(interface)
                    else:
                        raise RegisterException('No providers for interface {0}'.format(interface))
            finally:
                self.lock.release()
        return self._routing_with_weight(cache_key)

    def _get_providers_from_zk(self, path, interface, version, group):
        """
        从zk中根据interface获取到providers信息
        :param path:
        :param interface:
        :param version:
        :param group:
        :return:
        """
        cache_key = (interface, version, group)

        providers = self.zk.get_children(path, watch=self._watch_children)
        dubbo_providers = list(filter(lambda p: p['scheme'] == 'dubbo', map(parse_url, providers)))
        target_providers = [
            p for p in dubbo_providers
            if p.get('fields',{}).get('version') == version
            and p.get('fields',{}).get('group') == group
        ]
        if not target_providers:
            raise RegisterException('no providers for interface {} with version:{} in group:{}'.format(interface,version,group))
        self.hosts[cache_key] = [p['host'] for p in target_providers]
        self._raw_providers[interface] = dubbo_providers
        self._register_consumer(dubbo_providers)

        with self._interface_lock:
            self._subscribed_interfaces.add(interface)

    def _get_configurators_from_zk(self, interface):
        """
        试图从配置中取出权重相关的信息
        :param interface:
        :return:
        """
        configurators = self.zk.get_children(DUBBO_ZK_CONFIGURATORS.format(interface), watch=self._watch_configurators)
        if configurators:
            configurators = list(map(parse_url, configurators))
            conf = {}
            for configurator in configurators:
                conf[configurator['host']] = int(configurator['fields'].get('weight', 100))  # 默认100
            self.weights[interface] = conf

    def _watch_configurators(self, event):
        """
        监测某个interface中provider的权重的变化信息
        :param event:
        :return:
        """
        path = event.path
        logger.debug('zookeeper node changed: {}'.format(path))
        interface = path.split('/')[2]

        # 试图从配置中取出权重相关的信息
        configurators = self.zk.get_children(DUBBO_ZK_CONFIGURATORS.format(interface),
                                             watch=self._watch_configurators)
        if configurators:
            configurators = list(map(parse_url, configurators))
            conf = {}
            for configurator in configurators:
                conf[configurator['host']] = int(configurator['fields'].get('weight', 100))
            logger.debug('{} configurators: {}'.format(interface, conf))
            self.weights[interface] = conf
        else:
            logger.debug('No configurator for interface {}'.format(interface))
            self.weights[interface] = {}

    def _register_consumer(self, providers):
        """
        把本机注册到对应的interface的consumer上去
        :param providers:
        :return:
        """
        provider = providers[0]
        provider_fields = provider['fields']

        consumer = 'consumer://' + get_ip() + provider['path'] + '?'
        fields = {
            'application': self.application_name,
            'category': 'consumers',
            'check': 'false',
            'connected': 'true',
            'dubbo': provider_fields['dubbo'],
            'interface': provider_fields['interface'],
            'methods': provider_fields['methods'],
            'pid': get_pid(),
            'revision': provider_fields['revision'],
            'side': 'consumer',
            'timestamp': int(time.time() * 1000),
            'version': provider_fields.get('version'),
            'group': provider_fields.get('group'),
        }

        params = []
        for key, value in sorted(fields.items()):
            params.append('{0}={1}'.format(key, value))
        consumer += '&'.join(params)

        logger.debug('Create consumer {}'.format(fields))
        consumer_path = DUBBO_ZK_CONSUMERS.format(fields['interface'])
        self.zk.ensure_path(consumer_path)
        self.zk.create_async(consumer_path + '/' + quote(consumer, safe=''), ephemeral=True)

    def _routing_with_weight(self, cache_key):
        """
        根据接口名称以及配置好的权重信息获取一个host
        :param cache_key:
        :return:
        """
        hosts = self.hosts.get(cache_key,[])
        if not hosts:
            raise RegisterException('no providers for {}'.format(cache_key))
        # 此接口没有权重设置，使用朴素的路由算法
        interface = cache_key[0]
        if interface not in self.weights or not self.weights[interface]:
            return random.choice(hosts)

        weights = self.weights[interface]
        weighted_hosts = [(host, int(weights.get(host,100))) for host in hosts if int(weights.get(host,100)) >0 ]
        if not weighted_hosts:
            raise RegisterException('no available providers for [{}] (all weights are 0).'.format(cache_key))
        
        hosts_list,hosts_weight = zip(*weighted_hosts)
        total = sum(hosts_weight)
        hit = random.randint(0, total-1)
        cumulative = 0
        for i, w in enumerate(hosts_weight):
            cumulative += w
            if hit < cumulative:
                return hosts_list[i]
        return hosts_list[-1]

    def close(self):
        self.zk.stop()


if __name__ == '__main__':
    pass
