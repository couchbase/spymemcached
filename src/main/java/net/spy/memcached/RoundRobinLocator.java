package net.spy.memcached;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedNodeROImpl;
import net.spy.memcached.NodeLocator;


/**
 * NodeLocator implementation that round-robins across active nodes.
 */
public final class RoundRobinLocator implements NodeLocator {

  private int nodeIndex;
  private MemcachedNode[] nodes;

  public RoundRobinLocator(List<MemcachedNode> n) {
    super();
    nodes = n.toArray(new MemcachedNode[n.size()]);
  }

  private RoundRobinLocator(MemcachedNode[] n) {
    super();
    nodes = n;
  }

  /**
   * @param k Input key (which is ignored in round robin)
   * @return Next active node. If none of the nodes are active, the next node in
   * the list is returned. Never returns null.
   */
  @Override
  public synchronized MemcachedNode getPrimary(String k) {
    int i;
    for (i = nodeIndex; !nodes[i % nodes.length].isActive()
    	&& i < nodeIndex + nodes.length; i++) {}

    nodeIndex = (i + 1) % nodes.length;
    return nodes[i % nodes.length];
  }

  @Override
  public Iterator<MemcachedNode> getSequence(String k) {
    return new NodeIterator(nodeIndex);
  }

  @Override
  public Collection<MemcachedNode> getAll() {
    return Arrays.asList(nodes);
  }

  @Override
  public NodeLocator getReadonlyCopy() {
    MemcachedNode[] n = new MemcachedNode[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      n[i] = new MemcachedNodeROImpl(nodes[i]);
    }
    return new RoundRobinLocator(n);
  }

  @Override
  public void updateLocator(List<MemcachedNode> newNodes) {
    this.nodes = newNodes.toArray(new MemcachedNode[newNodes.size()]);
  }

  class NodeIterator implements Iterator<MemcachedNode> {

    private final int start;
    private int next = 0;

    public NodeIterator(int keyStart) {
      start = keyStart;
      next = start;
      computeNext();
      assert next >= 0 || nodes.length == 1 : "Starting sequence at "
          + start + " of " + nodes.length + " next is " + next;
    }

    @Override
    public boolean hasNext() {
      return next >= 0;
    }

    private void computeNext() {
      if (++next >= nodes.length) {
        next = 0;
      }
      if (next == start) {
        next = -1;
      }
    }

    @Override
    public MemcachedNode next() {
      try {
        return nodes[next];
      } finally {
        computeNext();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Can't remove a node");
    }
  }

}