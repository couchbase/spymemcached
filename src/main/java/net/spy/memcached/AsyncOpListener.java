package net.spy.memcached;

import net.spy.memcached.TimeoutListener.Method;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

public interface AsyncOpListener<T> {

  T beforeInvoke(Method method);

  void onBulkGetCompletion(T before, BulkGetFuture<?> future);

  void onGetCompletion(T before, GetFuture<?> future);

  void onOperationCompletion(T before, Method method, OperationFuture<?> future);
}
