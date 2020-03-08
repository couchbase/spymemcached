package net.spy.memcached.ops;

public interface OperationStateChangeObserver {

    void stateChanged(OperationState fromState, OperationState toState);
}
