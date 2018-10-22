package io.vasto;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

public class Connection implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(Connection.class.getName());

    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final ByteBuffer sizeBuffer;
    private final ConnectionPool connectionPool;

    public Connection(ConnectionPool connectionPool, String host, int port, int timeoutMs) throws IOException {
        this.connectionPool = connectionPool;
        this.socket = new Socket();
        this.socket.setTcpNoDelay(true);
        this.socket.setKeepAlive(true);
        this.socket.connect(new InetSocketAddress(host, port), timeoutMs);
        this.inputStream = socket.getInputStream();
        this.outputStream = socket.getOutputStream();
        this.sizeBuffer = ByteBuffer.allocate(4);
        this.sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public byte[] send(byte[] message) {

        byte[] bytes = null;

        try {

            this.sizeBuffer.clear();
            this.sizeBuffer.putInt(message.length);
            this.write(this.sizeBuffer.array(), 4);
            this.write(message, message.length);

            this.sizeBuffer.clear();
            this.readFully(this.sizeBuffer.array(), 4);
            int messageSize = this.sizeBuffer.getInt();
            bytes = new byte[messageSize];
            this.readFully(bytes, messageSize);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    private void write(byte[] buffer, int length) throws IOException {
        final int max = length;
        int pos = 0;
        int len;

        while (pos < max) {
            len = max - pos;

            if (len > 8192)
                len = 8192;

            this.outputStream.write(buffer, pos, len);
            pos += len;
        }
    }

    private void readFully(byte[] buffer, int length) throws IOException {
        int pos = 0;

        while (pos < length) {
            int count = this.inputStream.read(buffer, pos, length - pos);

            if (count < 0)
                throw new EOFException();

            pos += count;
        }
    }

    public void shutdown() {
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        this.connectionPool.returnConnection(this);
    }
}
