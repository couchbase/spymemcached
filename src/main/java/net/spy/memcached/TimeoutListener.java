package net.spy.memcached;

import java.util.EventListener;
import java.util.concurrent.Future;

import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.StoreType;

public interface TimeoutListener extends EventListener {

  /**
   * @param future may be {@code null} if from some non async operations.
   */
  void onTimeout(Method method, Future<?> future) throws Exception;

  enum Method {
    get, getAndTouch, getBulk, getBulkSome, gets, set, replace, add, append, prepend, touch, cas, incr, decr, delete;

    static Method from(Mutator mutator) {
      switch (mutator) {
        case incr:
          return incr;
        case decr:
          return decr;
        default:
          return null;
      }
    }

    static Method from(ConcatenationType concatenationType) {
      switch (concatenationType) {
        case append:
          return append;
        case prepend:
          return prepend;
        default:
          return null;
      }
    }

    static Method from(StoreType storeType) {
      switch (storeType) {
        case set:
          return set;
        case add:
          return add;
        case replace:
          return replace;
        default:
          return null;
      }
    }
  }
}
