package io.vasto;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ConnectionPool {

    private final int capacity;
    private String host;
    private int port;
    private GenericObjectPool<Connection> pool;

    public ConnectionPool(String address, int capacity, int timeoutMs) {
        this.capacity = capacity;
        int colonPosition = address.lastIndexOf(":");
        this.host = address.substring(0, colonPosition);
        this.port = Integer.parseInt(address.substring(colonPosition + 1));

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(1);
        config.setMaxTotal(capacity);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);

        ConnectionPool self = this;

        this.pool = new GenericObjectPool(new PooledObjectFactory<Connection>() {
            @Override
            public PooledObject<Connection> makeObject() throws Exception {
                Connection connection = new Connection(self, host, port, timeoutMs);
                return new DefaultPooledObject<Connection>(connection);
            }

            @Override
            public void destroyObject(PooledObject<Connection> p) throws Exception {
                p.getObject().shutdown();
            }

            @Override
            public boolean validateObject(PooledObject<Connection> p) {
                return true;
            }

            @Override
            public void activateObject(PooledObject<Connection> p) throws Exception {

            }

            @Override
            public void passivateObject(PooledObject<Connection> p) throws Exception {

            }
        }, config);
    }

    public final Connection getConnection() throws Exception {
        return pool.borrowObject();
    }

    public final void returnConnection(Connection conn) {
        pool.returnObject(conn);
    }

    public final void shutdown() {
        pool.close();
    }

}
