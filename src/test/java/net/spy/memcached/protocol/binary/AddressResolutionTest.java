package net.spy.memcached.protocol.binary;

import junit.framework.Assert;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.Operation;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AddressResolutionTest {
  @Test
  public void testResolveAddressWithHostname() throws Exception {
    BlockingQueue<Operation> queue = new ArrayBlockingQueue<>(1);
    MemcachedNode node = new BinaryMemcachedNodeImpl(new InetSocketAddress("memcached.org", 80),
        SocketChannel.open(), 1024, queue, queue, queue, 1024L, false,
        1024, 1024, new DefaultConnectionFactory());
      SocketAddress originalSocketAddress = node.getSocketAddress();
      SocketAddress resolvedSocketAddress = node.getSocketAddress(true);
      Assert.assertEquals("socket address with hostname couldn't be resolved",
          originalSocketAddress, resolvedSocketAddress);
  }

  @Test
  public void testResolveAddressWithAddress() throws Exception {
    BlockingQueue<Operation> queue = new ArrayBlockingQueue<>(1);
    MemcachedNode node = new BinaryMemcachedNodeImpl(
        new InetSocketAddress(InetAddress.getByName("memcached.org"), 80),
        SocketChannel.open(), 1024, queue, queue, queue, 1024L, false,
        1024, 1024, new DefaultConnectionFactory());
    SocketAddress originalSocketAddress = node.getSocketAddress();
    SocketAddress resolvedSocketAddress = node.getSocketAddress(true);
    Assert.assertEquals("socket address with address couldn't be resolved",
        originalSocketAddress, resolvedSocketAddress);
  }

  @Test
  public void testResolveAddressWithAddressAsHostname() throws Exception {
    BlockingQueue<Operation> queue = new ArrayBlockingQueue<>(1);
    MemcachedNode node = new BinaryMemcachedNodeImpl(
        new InetSocketAddress(InetAddress.getByName("memcached.org").getHostAddress(), 80),
        SocketChannel.open(), 1024, queue, queue, queue, 1024L, false,
        1024, 1024, new DefaultConnectionFactory());
    SocketAddress originalSocketAddress = node.getSocketAddress();
    SocketAddress resolvedSocketAddress = node.getSocketAddress(true);
    Assert.assertEquals("socket address with address as hostname couldn't be resolved",
        originalSocketAddress, resolvedSocketAddress);
  }
}
