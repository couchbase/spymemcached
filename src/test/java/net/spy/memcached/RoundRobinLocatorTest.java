package net.spy.memcached;

import java.util.Arrays;
import java.util.Collection;


/**
 * Test the RoundRobinLocatorTest.
 */
public class RoundRobinLocatorTest extends AbstractNodeLocationCase {

  private void setActive(boolean value, int ... nodes) {
    for (int n : nodes) {
      nodeMocks[n].expects(atLeastOnce()).method("isActive")
          .will(returnValue(value));
    }
  }

  @Override
  protected void setupNodes(int n) {
    super.setupNodes(n);
    locator = new RoundRobinLocator(Arrays.asList(nodes));
  }

  public void testPrimarySingleNodeActive() throws Exception {
    setupNodes(1);
    setActive(true, 0);
    assertSame(nodes[0], locator.getPrimary("a"));
    assertSame(nodes[0], locator.getPrimary("b"));
    assertSame(nodes[0], locator.getPrimary("c"));
  }

  public void testPrimarySingleNodeDown() throws Exception {
    setupNodes(1);
    setActive(false, 0);
    assertSame(nodes[0], locator.getPrimary("a"));
    assertSame(nodes[0], locator.getPrimary("b"));
    assertSame(nodes[0], locator.getPrimary("c"));
  }

  public void testPrimaryMultiNodeOneDown() throws Exception {
    setupNodes(2);
    setActive(false, 0);
    setActive(true, 1);
    assertSame(nodes[1], locator.getPrimary("a"));
    assertSame(nodes[1], locator.getPrimary("b"));
    assertSame(nodes[1], locator.getPrimary("c"));
  }

  public void testPrimaryMultiMixedDown() throws Exception {
    setupNodes(4);
    setActive(false, 0, 2);
    setActive(true, 1, 3);
    assertSame(nodes[1], locator.getPrimary("a"));
    assertSame(nodes[3], locator.getPrimary("b"));
    assertSame(nodes[1], locator.getPrimary("c"));
    assertSame(nodes[3], locator.getPrimary("a"));
  }

  public void testPrimaryMultiNodeAllActive() throws Exception {
    setupNodes(4);
    setActive(true, 0, 1, 2, 3);
    assertSame(nodes[0], locator.getPrimary("a"));
    assertSame(nodes[1], locator.getPrimary("b"));
    assertSame(nodes[2], locator.getPrimary("c"));
    assertSame(nodes[3], locator.getPrimary("d"));
    assertSame(nodes[0], locator.getPrimary("e"));
    assertSame(nodes[1], locator.getPrimary("f"));
    assertSame(nodes[2], locator.getPrimary("g"));
    assertSame(nodes[3], locator.getPrimary("h"));
    assertSame(nodes[0], locator.getPrimary("i"));
  }

  public void testPrimaryMultiNodeAllDown() throws Exception {
    setupNodes(4);
    setActive(false, 0, 1, 2, 3);
    assertSame(nodes[0], locator.getPrimary("a"));
    assertSame(nodes[1], locator.getPrimary("b"));
    assertSame(nodes[2], locator.getPrimary("c"));
    assertSame(nodes[3], locator.getPrimary("d"));
    assertSame(nodes[0], locator.getPrimary("e"));
    assertSame(nodes[1], locator.getPrimary("f"));
    assertSame(nodes[2], locator.getPrimary("g"));
    assertSame(nodes[3], locator.getPrimary("h"));
    assertSame(nodes[0], locator.getPrimary("i"));
  }

  public void testPrimaryClone() throws Exception {
    setupNodes(2);
    setActive(true, 0);
    assertEquals(nodes[0].toString(),
      locator.getReadonlyCopy().getPrimary("a").toString());
    assertEquals(nodes[0].toString(),
      locator.getReadonlyCopy().getPrimary("b").toString());
  }

  public void testAll() throws Exception {
    setupNodes(4);
    Collection<MemcachedNode> all = locator.getAll();
    assertEquals(4, all.size());
    assertTrue(all.contains(nodes[0]));
    assertTrue(all.contains(nodes[1]));
    assertTrue(all.contains(nodes[2]));
    assertTrue(all.contains(nodes[3]));
  }

  public void testAllClone() throws Exception {
    setupNodes(4);
    Collection<MemcachedNode> all = locator.getReadonlyCopy().getAll();
    assertEquals(4, all.size());
  }

  @Override
  public final void testCloningGetPrimary() {
    setupNodes(5);
    setActive(true, 0);
    assertTrue(locator.getReadonlyCopy().getPrimary("hi")
      instanceof MemcachedNodeROImpl);
  }

}
