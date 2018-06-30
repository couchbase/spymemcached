package net.spy.memcached;

import java.nio.channels.SelectionKey;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.MemcachedNode;

/**
 * A TimeoutCounterTest. Some asynchronous workflows may call get on the same OperationFuture
 * instance multiple times. This had a side-effect of resetting the ContinuousTimeout counter.
 */
public class TimeoutCounterTest extends ClientBaseCase {

    private int getContinuousTimeout() {
        MemcachedConnection conn = client.getConnection();
        for (SelectionKey sk : conn.selector.keys()) {
            MemcachedNode mn = (MemcachedNode) sk.attachment();
            return mn.getContinuousTimeout();
        }
        return -1;
    }

    // Fix for continuous timeout reset one subsequent get of future
    public void testTimeoutCounterGet() throws Exception {

        GetFuture<Object> future = client.asyncGet("k");

        // call get on the same future more than once
        // check if continuous timeout is still 1
        try {
            // force a timeout
            future.get(0, TimeUnit.NANOSECONDS);
            fail("did not timeout");
        } catch (Exception e) {
            // expected
        }

        assertEquals(1, getContinuousTimeout());

        try {
            // force a timeout
            future.get(0, TimeUnit.NANOSECONDS);
            fail("did not timeout");
        } catch (Exception e) {
            // expected
        }

        assertEquals(1, getContinuousTimeout());
    }

    // Fix for continuous timeout reset one subsequent get of future
    public void testTimeoutCounterSet() throws Exception {

        OperationFuture<Boolean> future = client.set("k", 500, "test");

        // call get on the same future more than once
        // check if continuous timeout is still 1
        try {
            // force a timeout
            future.get(0, TimeUnit.NANOSECONDS);
            fail("did not timeout");
        } catch (Exception e) {
            // expected
        }

        assertEquals(1, getContinuousTimeout());

        try {
            // force a timeout
            future.get(0, TimeUnit.NANOSECONDS);
            fail("did not timeout");
        } catch (Exception e) {
            // expected
        }

        assertEquals(1, getContinuousTimeout());
    }
}
