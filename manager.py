"""
client pool manager

previous:

    pool = ClientConnectionPool
    pool.stub.GRPCInterface(Company=company)

    client 内部需要维护一个方法集

usage:
    manager = Manager

    manager.register(pool)


    将方法集合传递给manager即可实现
    manager.GRPCInterface(Company)

    @send
    send(GRPCInterface, Company, *args)


    SingleMode
"""
import threading

import yaml


class Manager(object):
    _instance_lock = threading.Lock()
    _instance = None

    methods = {}
    pools = {}

    def __new__(cls, *args, **kwargs):
        if not getattr(Manager, "_instance"):
            with Manager._instance_lock:
                if not getattr(Manager, "_instance"):
                    Manager._instance = object.__new__(cls)
        return Manager._instance

    def __init__(self, config=None, *args, **kwargs):
        pass

    def register(self, *args):
        """
        注册一个连接池
        :param args: [class:ClientConnectionPool,]
        :return:
        """
        for pool in args:
            self._collect_methods(pool)

    def _collect_methods(self, pool):
        """
        注册所有连接池的方法
        :return:
        """
        for method in pool.methods:
            self.methods[method] = pool

    def __getattr__(self, item):
        if item in self.methods:
            method = getattr(self.methods[item], item, None)
            # print(self.methods[item].methods)
            if not method:
                raise AttributeError("[%s] not defined in %s" % (item, self.__class__))
            return method
        else:
            raise AttributeError("[%s] not defined in %s" % (item, self.__class__))
