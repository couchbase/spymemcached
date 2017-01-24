import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;

public class Main {
    public static void main(String[] args) throws IOException {
        List<InetSocketAddress> addrs = Arrays.asList(new InetSocketAddress("localhost", 11211));
        MemcachedClient client = new MemcachedClient(new BinaryConnectionFactory(), addrs);

        SerializingTranscoder transcoder  = new SerializingTranscoder();
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);

        byte[] data = new byte[2 * 1024 * 1024];
        while (true) {
            client.set("test", 60, data, transcoder);
        }
    }
}
