package net.spy.memcached;



import java.util.*;

/**
 * Uses a sorted array of values to represent the consistent hash ring of values that are associated with
 * memcached nodes are at that index.
 *
 * The method {@link #findCeilIndex(long)} finds the index in the array (the memcached node in the ring) at which the
 * first value is greater than or equal to the given long is.  If the long is greater than the maximum value
 * in the array then the memcached node at the first position in the array is returned (The first item in the ring)
 */
public class ArrayBasedCeilRing {

    private final long[] sortedNodePositions;
    private final MemcachedNode[] sortedNodes;
    private final Collection<MemcachedNode> allNodes;
    private final int lastIndexPosition;

    /**
     * Is Passed ({@see #nodes}) a map of longs to the associated memcached nodes.
     * This is used to create the sorted array ring and associated
     * memcached nodes at those array indexes.
     *
     * The {@see #allNodes} is a list of unique memcached nodes that make up the current cluster
     *
     * @param nodes A map of longs to memcached nodes that should populate the consistent hash ring.
     * @param allNodes The unique list of memcached nodes that are represented in the ring.
     */
    public ArrayBasedCeilRing(Map<Long, MemcachedNode> nodes, Collection<MemcachedNode> allNodes) {
        long[] sortedNodePositions = new long[nodes.size()];
        MemcachedNode[] sortedNodes = new MemcachedNode[nodes.size()];
        this.allNodes = new ArrayList(allNodes);

        Long[] sortedObjectNodePositions = nodes.keySet().toArray(new Long[nodes.size()]);
        Arrays.sort(sortedObjectNodePositions);

        int i = 0;
        for(Long position : sortedObjectNodePositions) {
            sortedNodePositions[i] = position;
            sortedNodes[i++] = nodes.get(position);
        }

        this.sortedNodePositions = sortedNodePositions;
        this.sortedNodes = sortedNodes;
        this.lastIndexPosition = nodes.size()-1;
    }

    /**
     * Returns the max value in the ring that a memcached node is located at.
     */
    public long getMaxPosition() {
        return sortedNodePositions[lastIndexPosition];
    }

    /**
     * Returns as a map, the memcached nodes associated to their positions
     * @return
     */
    public Map<Long,MemcachedNode> asMap() {
        Map<Long,MemcachedNode> map = new TreeMap<Long,MemcachedNode>();
        for(int i=0;i<sortedNodePositions.length;i++) {
            map.put(sortedNodePositions[i],sortedNodes[i]);
        }
        return map;
    }

    /**
     * Returns the list of memcached nodes that are represented in the current ring.
     * @return
     */
    public Collection<MemcachedNode> getAllNodes() {
        return allNodes;
    }

    public ArrayBasedCeilRing roClone() {
        Map<Long,MemcachedNode> nodes = new HashMap<Long, MemcachedNode>(sortedNodePositions.length,1.0f);
        for(int i=0;i<sortedNodePositions.length;i++) {
            nodes.put(sortedNodePositions[i],new MemcachedNodeROImpl(sortedNodes[i]));
        }
        List<MemcachedNode> allNodes = new ArrayList<MemcachedNode>(this.allNodes.size());
        for(MemcachedNode node : this.allNodes) {
            allNodes.add(new MemcachedNodeROImpl(node));
        }

        return new ArrayBasedCeilRing(nodes,allNodes);
    }

    public MemcachedNode findClosestNode(long key) {
        return sortedNodes[findCeilIndex(key)];
    }

    /**
     * Find the index in the array at which the first value greater than or equal
     * to the given hashVal is.   If hashVal is greater than the maximum value in the
     * array then 0 is returned (The first item in the ring).  If hashVal is less than or
     * equal to the first element in the array then 0 is returned (The first item in the ring).
     *
     * Uses a binary search type algorithm ( O(log n) ) to find the closest largest value in the array, to the
     * given hashVal
     *
     * @param hashVal The value to find the closest item
     * @return The index in the array that the closest largest item exists
     */
    public int findCeilIndex(final long hashVal) {
        return findCeilIndex(hashVal,sortedNodePositions,lastIndexPosition);
    }

    /**
     * Find the index in the array at which the first value greater than or equal
     * to the given hashVal is.   If hashVal is greater than the maximum value in the
     * array then 0 is returned (The first item in the ring).  If hashVal is less than or
     * equal to the first element in the array then 0 is returned (The first item in the ring).
     *
     * Uses a binary search type algorithm ( O(log n) ) to find the closest largest value in the array, to the
     * given hashVal
     *
     * @param hashVal The value to find the closest item
     * @return The index in the array that the closest largest item exists
     */
     static int findCeilIndex(final long hashVal, final long sortedNodePositions[], final int lastIndexPosition) {
        if (lastIndexPosition < 0) {
            throw new IllegalArgumentException("array cannot be empty");
        }

        int low = 0;
        int high = lastIndexPosition;

        // Check for edge cases
        if (hashVal > sortedNodePositions[high]) return 0;
        if (hashVal <= sortedNodePositions[low]) return 0;


        while(true) {
            // div by two
            int mid = (low + high) >>> 1;
            long midVal = sortedNodePositions[mid];
            if (midVal == hashVal) return mid;
            else if (midVal < hashVal) {
                low = mid + 1;
                if (low <= high && sortedNodePositions[low] >= hashVal) return low;
            }
            else if (midVal > hashVal) {
                high = mid - 1;
                if (high >= low && sortedNodePositions[high] < hashVal) return mid;
            }
        }
    }


}
