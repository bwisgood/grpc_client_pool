from grpc import ChannelConnectivity


class DefaultCallBackHandler(object):
    def __init__(self, channel):
        self.channel = channel

    def dispatch(self, channel, state):
        if state == ChannelConnectivity.TRANSIENT_FAILURE:
            return self.transient_failure(channel)
        elif state == ChannelConnectivity.SHUTDOWN:
            return self.shut_down(channel)
        elif state == ChannelConnectivity.CONNECTING:
            return self.connecting(channel)
        elif state == ChannelConnectivity.READY:
            return self.ready(channel)
        elif state == ChannelConnectivity.IDLE:
            return self.idle(channel)

    def shut_down(self, channel):
        channel.state = "SHUTDOWN"
        channel.reconnect()
        print("[%s] server error with shutdown" % channel.connect_id)

    def connecting(self, channel):
        channel.state = "CONNECTING"
        print("[%s] I am trying to connect server" % channel.connect_id)

    def ready(self, channel):
        channel.state = "READY"
        print("[%s] I am ready to send a request" % channel.connect_id)

    def transient_failure(self, channel):
        channel.state = "TRANSIENT_FAILURE"
        channel.reconnect()
        print("[%s] someting wrong with this channel" % channel.connect_id)

    def idle(self, channel):
        channel.state = "IDLE"
        print("[%s] waiting" % channel.connect_id)
