package net.spy.memcached.ops;

public interface OperationStateChangeObserver {

    void stateChanged(Operation operation, OperationState prevState, OperationState currentState, long timeFromPrevToCurrentStateMicros);
}
