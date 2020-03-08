package net.spy.memcached.ops;

import net.spy.memcached.metrics.MetricCollector;

import java.util.concurrent.TimeUnit;

/**
 * An {@link OperationStateChangeObserver} implementation for monitoring purposes.
 * */
public class MonitoringOperationStateChangeObserver implements OperationStateChangeObserver {

    private final MetricCollector metricCollector;
    private final Operation operation;
    private volatile long prevChangeTime;

    public MonitoringOperationStateChangeObserver(MetricCollector metricCollector, Operation operation) {
        this.metricCollector = metricCollector;
        this.operation = operation;
        this.prevChangeTime = System.nanoTime();
    }

    public void stateChanged(OperationState fromState, OperationState toState) {
        int durationMicros = (int) TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - prevChangeTime);
        prevChangeTime = System.nanoTime();

        String metricName = "time-from-" + fromState + "-to-" + toState;
        String perNodeMetricName = metricName + "-in-node-" + operation.getHandlingNode().getSocketAddress();

        metricCollector.updateHistogram(metricName, durationMicros);
        metricCollector.updateHistogram(perNodeMetricName, durationMicros);
    }
}