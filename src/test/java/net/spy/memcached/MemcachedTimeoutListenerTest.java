package net.spy.memcached;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.spy.memcached.TestConfig.PORT_NUMBER;
import static net.spy.memcached.TimeoutListener.Method.add;
import static net.spy.memcached.TimeoutListener.Method.append;
import static net.spy.memcached.TimeoutListener.Method.decr;
import static net.spy.memcached.TimeoutListener.Method.delete;
import static net.spy.memcached.TimeoutListener.Method.get;
import static net.spy.memcached.TimeoutListener.Method.getAndTouch;
import static net.spy.memcached.TimeoutListener.Method.getBulk;
import static net.spy.memcached.TimeoutListener.Method.getBulkSome;
import static net.spy.memcached.TimeoutListener.Method.incr;
import static net.spy.memcached.TimeoutListener.Method.prepend;
import static net.spy.memcached.TimeoutListener.Method.replace;
import static net.spy.memcached.TimeoutListener.Method.set;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;
import net.spy.memcached.TimeoutListener.Method;

public class MemcachedTimeoutListenerTest extends TestCase {

  private MemcachedClient client = null;
  private Set<Method> methodSet = new HashSet<Method>();

  @Override
  protected void setUp() throws IOException {
    client = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses("1.1.1.1:" + PORT_NUMBER));
    client.addTimeoutListener(new TimeoutListener() {

      public void onTimeout(Method method, Future<?> future) {
        methodSet.add(method);
      }
    });
  }

  @Override
  protected void tearDown() {
    if (client != null) {
      try {
        client.shutdown();
      } catch (NullPointerException e) {
        // This is a workaround for a disagreement betweewn how things
        // should work in eclipse and buildr. My plan is to upgrade to
        // junit4 all around and write some tests that are a bit easier
        // to follow.

        // The actual problem here is a client that isn't properly
        // initialized is attempting to be shut down.
      }
    }
  }

  public void testTimeoutListener() throws ExecutionException, InterruptedException {
    try {
      client.get("test");
      fail();
    } catch (OperationTimeoutException e) {
    }
    assertTrue(methodSet.contains(get));

    try {
      client.set("test", 1, 1).get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(set));

    try {
      client.getBulk("test1", "test2");
      fail();
    } catch (OperationTimeoutException e) {
    }
    assertTrue(methodSet.contains(getBulk));

    client.asyncGetBulk("test3").getSome(1, MILLISECONDS);
    assertTrue(methodSet.contains(getBulkSome));

    try {
      client.getAndTouch("test1", 1);
      fail();
    } catch (OperationTimeoutException e) {
    }
    assertTrue(methodSet.contains(getAndTouch));

    try {
      client.incr("test1", 1);
      fail();
    } catch (OperationTimeoutException e) {
    }
    assertTrue(methodSet.contains(incr));

    try {
      client.decr("test1", 1);
      fail();
    } catch (OperationTimeoutException e) {
    }
    assertTrue(methodSet.contains(decr));

    try {
      client.append("test1", 1).get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(append));

    try {
      client.prepend("test1", 1).get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(prepend));

    try {
      client.add("test1", 1, "value").get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(add));

    try {
      client.replace("test1", 1, "value").get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(replace));

    try {
      client.delete("test1").get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(delete));

    assertTrue(methodSet.remove(incr));
    assertTrue(methodSet.remove(decr));

    try {
      client.asyncIncr("test1", 1).get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(incr));

    try {
      client.asyncDecr("test1", 1).get(1, MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    assertTrue(methodSet.contains(decr));
  }
}
