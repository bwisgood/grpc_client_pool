import time
from random import randint
from threading import Lock
from contextlib import contextmanager

from grpc import insecure_channel, intercept_channel
from grpc import ChannelConnectivity

from .callback_handler import DefaultCallBackHandler
from .utils import weight_random

lock = Lock()


class ClientConnectionPool:
    """
    客户端连接池
    """

    _STATUS_OK = 0
    _STATUS_WARNING = 1
    _STATUS_ERROR = 2
    _STATUS_SHUTDOWN = 3

    callback_handler = DefaultCallBackHandler
    error_handler = None

    def __init__(self, host="localhost", port=9100, pool_size=5, weights=None, intercept=None, stub_cls=None, **kwargs):
        """
        初始化连接池对象
        :param host: ip
        :param port: 端口
        :param pool_size: pool大小
        :param intercept: 头部拦截器
        """
        self.methods = set()
        self.pool = []
        self.hosts = host if isinstance(host, list) else [host, ]
        self.ports = port if isinstance(port, list) else [port, ]
        if len(self.hosts) != len(self.ports):
            raise Exception("length of host[%d] must equal length of port[%d]" % (len(self.hosts), len(self.ports)))

        if len(self.hosts) > 1:
            self.distribute_mode = True
        else:
            self.distribute_mode = False

        if not weights:
            self.weights = [1 for _ in range(len(self.hosts))]

        self.pool_size = pool_size
        self.intercept = intercept
        self.reconnect_loop_time = kwargs.pop("reconnect_loop_time", 5)
        # if self.callback_handler is not None:
        #     self.callback_handler = self.callback_handler()

        self.stub_cls = stub_cls
        # 初始化连接池
        self._init_pool()
        self.status = self._STATUS_OK

    def _init_pool(self):
        """
        连接池初始化方法
        :return:
        """
        self.pool = set()

        for size in range(self.pool_size):
            n = randint(0, len(self.hosts) - 1)
            host = self.hosts[n]
            port = self.ports[n]
            weight = self.weights[n]

            channel = ExtendChannel(self, ExtendChannel.connect_id, host, port, self.callback_handler, self.intercept,
                                    self.reconnect_loop_time, self.stub_cls, weight=weight)
            ExtendChannel.connect_id += 1

            self.pool.add(channel)

    def get_connection(self, conn_id):
        """
        通过连接id获取连接对象
        :param conn_id: 连接对象的id
        :return:
        """
        for c in self.pool:
            if c.connect_id == conn_id:
                return c
        return None

    def get_one_connection(self):
        """
        随机获取一个连接对象
        :return:
        """
        with lock:
            ready_rand = []
            for conn in self.pool:
                if conn.state in ("READY", "IDLE"):
                    ready_rand.append(conn)

            if not ready_rand:
                raise BlockingIOError("All connection are busy")
            return weight_random(self.pool, key="weight")

    def get_connection_state(self, conn_id):
        """
        获取某个id的连接的连接状态
        :param conn_id:
        :return:
        """
        c = self.get_connection(conn_id)
        if c:
            return c.state

    def close(self, conn_id) -> None:
        """
        关闭某个id 的连接
        :param conn_id:
        :return:
        """
        c = self.get_connection(conn_id)
        if c:
            c.close()

    def close_all(self):
        """
        关闭连接池中的所有连接
        :return:
        """
        for c in self.pool:
            c.close()

    def start_all(self):
        """
        重新开启连接池中的所有连接
        :return:
        """
        self._init_pool()

    def get_all_channel_state(self):
        d = {}
        for channel in self.pool:
            d[channel.connect_id] = channel.state
        return d

    def get_stub(self, stub_cls):
        """
        封装获取客户端存根方法，不用每次都去get一个conn再去创建
        :param stub_cls:
        :return:
        """
        conn = self.get_one_connection()
        stub = stub_cls(conn)
        return stub

    def __getattr__(self, item):
        if item in self.methods:
            # return self.methods[item]
            # 获取一个channel
            # 使用这个channel的stub去发送请求
            c = self.get_one_connection()
            method = getattr(c.stub, item, None)
            if not method:
                raise AttributeError("[%s] not defined in %s" % (item, self.__class__))
            return method
        else:
            raise AttributeError("[%s] not defined in %s" % (item, self.__class__))


class ExtendChannel(object):
    """
    普通的channel回调中没有连接对象参数，所以把callback加到Channel上以区分
    """
    extra_state = ['INITIALIZING', "DEPRECATED", "BUSY"]

    connect_id = 0

    def __init__(self, pool, connect_id, host, port, callback_handler, intercept, reconnect_loop_time, stub_cls=None,
                 **kwargs):
        """
        初始化
        :param connect_id: 连接id
        :param host: 域名
        :param port: 端口
        :param callback_handler: 回调handler
        :param intercept: 头部拦截器
        """
        if pool:
            self.pool = pool

        self.connect_id = connect_id
        self.intercept = intercept
        self.host = host
        self.port = port
        self._channel = self.connect()
        self.callback_handler = callback_handler(self._channel)
        self.reconnect_loop_time = reconnect_loop_time
        if not self._channel:
            self.reconnect()
        self._channel.subscribe(self.callback)
        self._state = "IDLE"

        if stub_cls:
            self.stub = self.init_stub(stub_cls)
            self.connect_id = str(self.connect_id) + ":" + stub_cls.__name__

        self._weight = kwargs.pop("weight", 1)

    def reconnect(self):
        """
        重新连接
        :return:
        """
        while True:
            time.sleep(self.reconnect_loop_time)
            try:
                self._channel = self.connect()
            except:
                pass
            else:
                if self._channel:
                    self._state = "IDLE"
                    break

    def connect(self):
        """
        连接
        :return:
        """
        MB = 1024 * 1024
        GRPC_CHANNEL_OPTIONS = [('grpc.max_message_length', 64 * MB), ('grpc.max_receive_message_length', 64 * MB)]

        channel = insecure_channel("{}:{}".format(self.host, self.port), options=GRPC_CHANNEL_OPTIONS)
        if not self.intercept:
            return channel
        return intercept_channel(channel, self.intercept)

    def close(self):
        """
        关闭
        :return:
        """
        self._channel.close()
        self._state = "DEPRECATED"

    @property
    def state(self):
        """
        连接状态
        :return:
        """
        return self._state

    @state.setter
    def state(self, value):
        """
        不在ChannelConnectivity或extra_state中则不允许赋值
        :param value:
        :return:
        """
        temp = getattr(ChannelConnectivity, value, None)
        if temp is None and value not in self.extra_state:
            raise ValueError("Value Name Must in ChannelConnectivity")
        self._state = value

    @property
    def weight(self):
        return self._weight

    @weight.setter
    def weight(self, val):
        self._weight = val

    def callback(self, *args, **kwargs):
        """
        回调
        :return:
        """
        state = args[0]
        if self.callback_handler:
            with lock:
                return self.callback_handler.dispatch(self, state)

    def _busy(self):
        with lock:
            self._state = "BUSY"

    def _free(self):
        with lock:
            self._state = "IDLE"

    @contextmanager
    def use(self):
        self._busy()
        yield
        self._free()

    def init_stub(self, stub_cls):
        temp = stub_cls(self._channel)
        self.notify(temp)
        return temp

    def notify(self, stub_instance):
        if self.pool:
            for k, v in stub_instance.__dict__.items():
                if not k.startswith("__"):
                    self.pool.methods.add(k)

    def __getattr__(self, item):
        return getattr(self._channel, item, None)
