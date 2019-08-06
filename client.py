import time
from random import choice
from threading import Lock
from contextlib import contextmanager

from grpc import insecure_channel, intercept_channel
from grpc import ChannelConnectivity

from callback_handler import DefaultCallBackHandler

lock = Lock()


class ClientConnectionPool:
    """
    客户端连接池
    """
    callback_handler = DefaultCallBackHandler

    def __init__(self, host="localhost", port=9100, pool_size=5, intercept=None, **kwargs):
        """
        初始化连接池对象
        :param host: ip
        :param port: 端口
        :param pool_size: pool大小
        :param intercept: 头部拦截器
        """
        self.pool = []
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.intercept = intercept
        self.reconnect_loop_time = kwargs.pop("reconnect_loop_time", 5)
        if self.callback_handler is not None:
            self.callback_handler = self.callback_handler()

        # 初始化连接池
        self._init_pool()

    def _init_pool(self):
        """
        连接池初始化方法
        :return:
        """
        self.pool = set()
        for size in range(self.pool_size):
            channel = ExtendChannel(size, self.host, self.port, self.callback_handler, self.intercept,
                                    self.reconnect_loop_time)

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
            return choice(ready_rand)

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


class ExtendChannel(object):
    """
    普通的channel回调中没有连接对象参数，所以把callback加到Channel上以区分
    """
    extra_state = ['INITIALIZING', "DEPRECATED", "BUSY"]

    def __init__(self, connect_id, host, port, callback_handler, intercept, reconnect_loop_time):
        """
        初始化
        :param connect_id: 连接id
        :param host: 域名
        :param port: 端口
        :param callback_handler: 回调handler
        :param intercept: 头部拦截器
        """
        self.connect_id = connect_id
        self.callback_handler = callback_handler
        self.intercept = intercept
        self.host = host
        self.port = port
        self._channel = self.connect()
        self.reconnect_loop_time = reconnect_loop_time
        if not self._channel:
            self.reconnect()
        self._channel.subscribe(self.callback)
        self._state = "IDLE"

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
        channel = insecure_channel("{}:{}".format(self.host, self.port))
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

    def __getattr__(self, item):
        return getattr(self._channel, item, None)
