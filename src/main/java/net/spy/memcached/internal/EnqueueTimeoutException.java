package net.spy.memcached.internal;

import net.spy.memcached.ops.Operation;

public class EnqueueTimeoutException extends IllegalStateException {

  private final Operation op;

  public EnqueueTimeoutException(String s, Operation op) {
    super(s);
    this.op = op;
  }

  public Operation getOp() {
    return op;
  }
}
