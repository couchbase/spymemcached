/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
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

package net.spy.memcached.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.metrics.MetricCollector;
import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.TapOpcode;

/**
 * Base class for operation factories.
 *
 * <p>
 * There is little common code between OperationFactory implementations, but
 * some exists, and is complicated and likely to cause problems.
 * </p>
 */
public abstract class BaseOperationFactory implements OperationFactory {

  private final MetricCollector metricCollector;

  public BaseOperationFactory(MetricCollector metricCollector) {
    this.metricCollector = metricCollector;
  }

  protected abstract NoopOperation createNoop(OperationCallback cb);

  protected abstract DeleteOperation createDelete(String key, DeleteOperation.Callback callback);

  protected abstract DeleteOperation createDelete(String key, long cas, DeleteOperation.Callback callback);

  protected abstract UnlockOperation createUnlock(String key, long casId, OperationCallback operationCallback);

  protected abstract ObserveOperation createObserve(String key, long casId, int index, ObserveOperation.Callback operationCallback);

  protected abstract FlushOperation createFlush(int delay, OperationCallback operationCallback);

  protected abstract GetAndTouchOperation createGetAndTouch(String key, int expiration, GetAndTouchOperation.Callback cb);

  protected abstract GetOperation createGet(String key, GetOperation.Callback callback);

  protected abstract ReplicaGetOperation createReplicaGet(String key, int index, ReplicaGetOperation.Callback callback);

  protected abstract ReplicaGetsOperation createReplicaGets(String key, int index, ReplicaGetsOperation.Callback callback);

  protected abstract GetlOperation createGetl(String key, int exp, GetlOperation.Callback callback);

  protected abstract GetsOperation createGets(String key, GetsOperation.Callback callback);

  protected abstract GetOperation createGet(Collection<String> keys, GetOperation.Callback cb);

  protected abstract StatsOperation createKeyStats(String key, StatsOperation.Callback cb);

  protected abstract MutatorOperation createMutate(Mutator m, String key, long by, long def, int exp, OperationCallback cb);

  protected abstract StatsOperation createStats(String arg, StatsOperation.Callback cb);

  protected abstract StoreOperation createStore(StoreType storeType, String key, int flags, int exp, byte[] data, StoreOperation.Callback cb);

  protected abstract TouchOperation createTouch(String key, int expiration, OperationCallback cb);

  protected abstract ConcatenationOperation createCat(ConcatenationType catType, long casId, String key, byte[] data, OperationCallback cb);

  protected abstract CASOperation createCas(StoreType t, String key, long casId, int flags, int exp, byte[] data, StoreOperation.Callback cb);

  protected abstract VersionOperation createVersion(OperationCallback cb);

  protected abstract SASLMechsOperation createSaslMechs(OperationCallback cb);

  protected abstract SASLAuthOperation createSaslAuth(String[] mech, String serverName, Map<String, ?> props, CallbackHandler cbh, OperationCallback cb);

  protected abstract SASLStepOperation createSaslStep(String[] mech, byte[] challenge, String serverName, Map<String, ?> props, CallbackHandler cbh, OperationCallback cb);

  protected abstract TapOperation createTapBackfill(String id, long date, OperationCallback cb);

  protected abstract TapOperation createTapCustom(String id, RequestMessage message, OperationCallback cb);

  protected abstract TapOperation createTapAck(TapOpcode opcode, int opaque, OperationCallback cb);

  protected abstract TapOperation createTapDump(String id, OperationCallback cb);

  protected abstract Collection<? extends Operation> cloneGet(KeyedOperation op);

  public NoopOperation noop(OperationCallback cb) {
    return monitor(createNoop(cb));
  }

  public DeleteOperation delete(String key, DeleteOperation.Callback callback) {
    return monitor(createDelete(key, callback));
  }

  public DeleteOperation delete(String key, long cas, DeleteOperation.Callback callback) {
    return monitor(createDelete(key, cas, callback));
  }

  public UnlockOperation unlock(String key, long casId, OperationCallback operationCallback) {
    return monitor(createUnlock(key, casId, operationCallback));
  }

  public ObserveOperation observe(String key, long casId, int index, ObserveOperation.Callback operationCallback) {
    return monitor(createObserve(key, casId, index, operationCallback));
  }

  public FlushOperation flush(int delay, OperationCallback operationCallback) {
    return monitor(createFlush(delay, operationCallback));
  }

  public GetAndTouchOperation getAndTouch(String key, int expiration, GetAndTouchOperation.Callback cb) {
    return monitor(createGetAndTouch(key, expiration, cb));
  }

  public GetOperation get(String key, GetOperation.Callback callback) {
    return monitor(createGet(key, callback));
  }

  public ReplicaGetOperation replicaGet(String key, int index, ReplicaGetOperation.Callback callback) {
    return monitor(createReplicaGet(key, index, callback));
  }

  public ReplicaGetsOperation replicaGets(String key, int index, ReplicaGetsOperation.Callback callback) {
    return monitor(createReplicaGets(key, index, callback));
  }

  public GetlOperation getl(String key, int exp, GetlOperation.Callback callback) {
    return monitor(createGetl(key, exp, callback));
  }

  public GetsOperation gets(String key, GetsOperation.Callback callback) {
    return monitor(createGets(key, callback));
  }

  public GetOperation get(Collection<String> keys, GetOperation.Callback cb) {
    return monitor(createGet(keys, cb));
  }

  public StatsOperation keyStats(String key, StatsOperation.Callback cb) {
    return monitor(createKeyStats(key, cb));
  }

  public MutatorOperation mutate(Mutator m, String key, long by, long def, int exp, OperationCallback cb) {
    return monitor(createMutate(m, key,by, def, exp, cb));
  }

  public StatsOperation stats(String arg, StatsOperation.Callback cb) {
    return monitor(createStats(arg, cb));
  }

  public StoreOperation store(StoreType storeType, String key, int flags, int exp, byte[] data, StoreOperation.Callback cb) {
    return monitor(createStore(storeType, key, flags, exp, data, cb));
  }

  public TouchOperation touch(String key, int expiration, OperationCallback cb) {
    return monitor(createTouch(key, expiration, cb));
  }

  public ConcatenationOperation cat(ConcatenationType catType, long casId, String key, byte[] data, OperationCallback cb) {
    return monitor(createCat(catType, casId, key, data, cb));
  }

  public CASOperation cas(StoreType t, String key, long casId, int flags, int exp, byte[] data, StoreOperation.Callback cb) {
    return monitor(createCas(t, key, casId, flags, exp, data, cb));
  }

  public VersionOperation version(OperationCallback cb) {
    return monitor(createVersion(cb));
  }

  public SASLMechsOperation saslMechs(OperationCallback cb) {
    return monitor(createSaslMechs(cb));
  }

  public SASLAuthOperation saslAuth(String[] mech, String serverName, Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
    return monitor(createSaslAuth(mech, serverName, props, cbh, cb));
  }

  public SASLStepOperation saslStep(String[] mech, byte[] challenge, String serverName, Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
    return monitor(createSaslStep(mech, challenge, serverName, props, cbh, cb));
  }

  public TapOperation tapBackfill(String id, long date, OperationCallback cb) {
    return monitor(createTapBackfill(id, date, cb));
  }

  public TapOperation tapCustom(String id, RequestMessage message, OperationCallback cb) {
    return monitor(createTapCustom(id, message, cb));
  }

  public TapOperation tapAck(TapOpcode opcode, int opaque, OperationCallback cb) {
    return monitor(createTapAck(opcode, opaque, cb));
  }

  public TapOperation tapDump(String id, OperationCallback cb) {
    return monitor(createTapDump(id, cb));
  }

  public Collection<Operation> clone(KeyedOperation op) {
    assert (op.getState() == OperationState.WRITE_QUEUED || op.getState()
        == OperationState.RETRY) : "Who passed me an operation in the "
        + op.getState() + "state?";
    assert !op.isCancelled() : "Attempted to clone a canceled op";
    assert !op.hasErrored() : "Attempted to clone an errored op";

    Collection<Operation> rv = new ArrayList<Operation>(op.getKeys().size());
    if (op instanceof GetOperation) {
      rv.addAll(cloneGet(op));
    } else if (op instanceof ReplicaGetOperation) {
      rv.addAll(cloneGet(op));
    } else if (op instanceof ReplicaGetsOperation) {
      ReplicaGetsOperation.Callback callback =
        (ReplicaGetsOperation.Callback) op.getCallback();
      for (String k : op.getKeys()) {
        rv.add(replicaGets(k,
          ((ReplicaGetsOperation) op).getReplicaIndex(), callback));
      }
    } else if (op instanceof GetsOperation) {
      GetsOperation.Callback callback =
          (GetsOperation.Callback) op.getCallback();
      for (String k : op.getKeys()) {
        rv.add(gets(k, callback));
      }
    } else if (op instanceof CASOperation) {
      CASOperation cop = (CASOperation) op;
      rv.add(cas(cop.getStoreType(), first(op.getKeys()), cop.getCasValue(),
          cop.getFlags(), cop.getExpiration(), cop.getData(),
          (StoreOperation.Callback) cop.getCallback()));
    } else if(op instanceof DeleteOperation) {
      rv.add(delete(first(op.getKeys()),
          (DeleteOperation.Callback)op.getCallback()));
    } else if (op instanceof MutatorOperation) {
      MutatorOperation mo = (MutatorOperation) op;
      rv.add(mutate(mo.getType(), first(op.getKeys()), mo.getBy(),
          mo.getDefault(), mo.getExpiration(), op.getCallback()));
    } else if (op instanceof StoreOperation) {
      StoreOperation so = (StoreOperation) op;
      rv.add(store(so.getStoreType(), first(op.getKeys()), so.getFlags(),
          so.getExpiration(), so.getData(),
          (StoreOperation.Callback) op.getCallback()));
    } else if (op instanceof ConcatenationOperation) {
      ConcatenationOperation c = (ConcatenationOperation) op;
      rv.add(cat(c.getStoreType(), c.getCasValue(), first(op.getKeys()),
          c.getData(), c.getCallback()));
    } else if(op instanceof GetAndTouchOperation) {
      GetAndTouchOperation gt = (GetAndTouchOperation) op;
      rv.add(getAndTouch(first(gt.getKeys()), gt.getExpiration(),
        (GetAndTouchOperation.Callback) gt.getCallback()));
    } else if (op instanceof TouchOperation) {
      TouchOperation tt = (TouchOperation) op;
      rv.add(touch(first(tt.getKeys()), tt.getExpiration(), tt.getCallback()));
    } else if (op instanceof GetlOperation) {
      GetlOperation gl = (GetlOperation) op;
      rv.add(getl(first(gl.getKeys()), gl.getExpiration(),
          (GetlOperation.Callback) gl.getCallback()));
    } else if (op instanceof ObserveOperation) {
      ObserveOperation oo = (ObserveOperation) op;
      rv.add(observe(first(oo.getKeys()), oo.getCasValue(), oo.getIndex(),
        (ObserveOperation.Callback) oo.getCallback()));
    } else {
      assert false : "Unhandled operation type: " + op.getClass();
    }
    if (op instanceof VBucketAware) {
      VBucketAware vop = (VBucketAware) op;
      if (!vop.getNotMyVbucketNodes().isEmpty()) {
        for (Operation operation : rv) {
          if (operation instanceof VBucketAware) {
            Collection<MemcachedNode> notMyVbucketNodes =
                vop.getNotMyVbucketNodes();
            ((VBucketAware) operation).setNotMyVbucketNodes(notMyVbucketNodes);
          }
        }
      }
    }
    return rv;
  }

  private String first(Collection<String> keys) {
    return keys.iterator().next();
  }

  /**
   * Add a monitoring observer to the given operation
   * */
  private <O extends Operation> O monitor(O operation) {
    operation.addStateObserver(new MonitoringOperationStateChangeObserver(metricCollector));
    return operation;
  }
}
