import threading
import importlib

import yaml

from client import ClientConnectionPool


class Manager(object):
    _instance_lock = threading.Lock()
    _instance = None

    methods = {}
    pools = set()

    def __new__(cls, *args, **kwargs):
        if not getattr(Manager, "_instance"):
            with Manager._instance_lock:
                if not getattr(Manager, "_instance"):
                    Manager._instance = object.__new__(cls)
        return Manager._instance

    def __init__(self, config=None, *args, **kwargs):

        if config:
            with open(config) as cfg:
                data = yaml.full_load(cfg)

            manager = data.get("manager")

            for pool in manager:
                hosts = []
                ports = []
                weight = []
                size = 3
                stub = None
                intercept = None
                for k, v in pool.items():
                    if k == "host":
                        hosts.append(v)
                    elif k == "port":
                        ports.append(v)
                    elif k == "weight":
                        weight.append(v)
                    elif k == "size":
                        size = v
                    elif k == "stub":
                        if v:
                            module_path, class_name = v.rsplit('.', 1)

                            modle = importlib.import_module(module_path)
                            meth = getattr(modle, class_name)
                            stub = meth
                    elif k == "intercept":

                        if v:
                            module_path, class_name = v.rsplit('.', 1)

                            modle = importlib.import_module(module_path)
                            meth = getattr(modle, class_name)
                            intercept = meth
                p = ClientConnectionPool(hosts=hosts, ports=ports, pool_size=size, stub_cls=stub, intercept=intercept)
                self.register(p)

    def register(self, *args):
        """
        注册一个连接池
        :param args: [class:ClientConnectionPool,]
        :return:
        """
        for pool in args:
            self.pools.add(pool)
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
            if not method:
                raise AttributeError("[%s] not defined in %s" % (item, self.__class__))
            return method
        else:
            raise AttributeError("[%s] not defined in %s" % (item, self.__class__))
