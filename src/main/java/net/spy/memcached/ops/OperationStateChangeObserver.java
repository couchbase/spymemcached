package net.spy.memcached.ops;

public interface OperationStateChangeObserver {

    void stateChanged(Operation operation, OperationState fromState, OperationState toState);
}
