/**
 * (c) Copyright 2019 freiheit.com technologies GmbH
 *
 * Created on 2019-09-25 by Marco Kortkamp (marco.kortkamp@freiheit.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.test;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

/**
 * A manual test which tries to trigger a NPE in the fillWriteBuffer
 * method, when large session passed to the memcached with very high
 * frequency.
 *
 * The initial verison of this test was provided in the following bug report.
 * @see <a href="https://github.com/couchbase/spymemcached/pull/17/commits/d29fea258f6595922b19667efd39a41611d0c0ec">Bug Report</a>
 *
 * @author Marco Kortkamp (marco.kortkamp@freiheit.com)
 */
public class FillWriteBufferNPETest {
    private static final String MEMCACHED_HOST = "localhost";
    private static final int MEMCACHED_PORT = 11211;
    private static final int SLEEP_MS = 1;

    /**
     * In order to run this test you need a memcached instance.
     * It can e.g. be started by:
     *     $ memcached -d -p 11211 -u memcached -m 64 -c 1024
     *
     * Experiments were conducted with the VM-Options "-Xmx4G".
     */
    public static void main(final String[] args) throws IOException, InterruptedException {
        final MemcachedClient client = new MemcachedClient(new BinaryConnectionFactory(),
                Collections.singletonList(new InetSocketAddress(MEMCACHED_HOST, MEMCACHED_PORT))
        );

        final SerializingTranscoder transcoder  = new SerializingTranscoder();
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);

        final byte[] data = new byte[2 * 1024 * 1024];
        int i = 0;
        try {
            while (true) {
                client.set("test", 60, data, transcoder);
                /*
                 * If this sleep is not in, the memcached ocassionally closes
                 * the socket with "java.io.IOException: Connection reset by peer"
                 * and during the reconnect phase the while loop shovels us into
                 * a an "java.lang.OutOfMemoryError: Java heap space".
                 * I'm not sure if this is a remaining issue.
                 */
                Thread.sleep(SLEEP_MS);
                if (i > 0 && i % 100 == 0) {
                    System.out.println(i);
                }
                i++;
            }
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }
}
