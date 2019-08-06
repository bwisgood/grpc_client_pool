# Abstract
This is a base thread pool to catch grpc server connection

# How to use
1. Create a `ClientConnectionPool` in your client
2. use `ClientConnectionPool.get_one_connection()` to allocate a spare connection.
3. use this connection to create your stub

# More

- `ClientConnectionPool.get_connection(conn_id)`

Get a connection by connection id

- `ClientConnectionPool.get_connection_state(conn_id)`

Get connection state by connection id

    Note: Also you can define your own callback handler base on class:DefaultCallBackHandler
