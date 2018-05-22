package net.spy.memcached;

import java.util.Iterator;

/**
 * Implements an Iterator which the KetamaNodeLoctaor may return to a client for
 * iterating through alternate nodes for a given key.
 */
public class ArrayBasedCeilRingIterator  implements Iterator<MemcachedNode> {

    private final String key;
    private long hashVal;
    private int remainingTries;
    private int numTries = 0;
    private final HashAlgorithm hashAlg;
    private final ArrayBasedCeilRing ring;

    /**
     * Create a new ArrayBasedCeilRingIterator to be used by a client for an operation.
     *
     * @param k the key to iterate for
     * @param t the number of tries until giving up
     * @param hashAlg the hash algorithm to use when selecting within the
     *          continuumq
     */
    protected ArrayBasedCeilRingIterator(final String k, final int t,
                             final HashAlgorithm hashAlg, final ArrayBasedCeilRing ring) {
        super();
        this.ring = ring;
        this.hashAlg = hashAlg;
        hashVal = hashAlg.hash(k);
        remainingTries = t;
        key = k;
    }

    private void nextHash() {
        // this.calculateHash(Integer.toString(tries)+key).hashCode();
        long tmpKey = hashAlg.hash((numTries++) + key);
        // This echos the implementation of Long.hashCode()
        hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
        hashVal &= 0xffffffffL; /* truncate to 32-bits */
        remainingTries--;
    }

    public boolean hasNext() {
        return remainingTries > 0;
    }

    public MemcachedNode next() {
        try {
            return ring.findClosestNode(hashVal);
        } finally {
            nextHash();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

}
