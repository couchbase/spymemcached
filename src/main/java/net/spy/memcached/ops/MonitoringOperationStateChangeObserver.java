package net.spy.memcached.ops;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.metrics.MetricCollector;

/**
 * An {@link OperationStateChangeObserver} implementation for monitoring purposes.
 * */
public class MonitoringOperationStateChangeObserver implements OperationStateChangeObserver {

    private static final CachedMetricNames cachedMetricNames = new CachedMetricNames();

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

        String metricName = cachedMetricNames.getOrCreateMetricName(fromState, toState);
        String perNodeMetricName = cachedMetricNames.getOrCreateMetricName(fromState, toState, operation.getHandlingNode().getSocketAddress());

        metricCollector.updateHistogram(metricName, durationMicros);
        metricCollector.updateHistogram(perNodeMetricName, durationMicros);
    }

    /**
     * A simple cache for the metrics names, to avoid creating them per state-change per operation
     * */
    private static class CachedMetricNames {

        // (prevState, newState) -> metricName
        private final Map<OperationState, Map<OperationState, String>> metricKeyToMetricName = new HashMap<OperationState, Map<OperationState, String>>();
        // (prevState, newState, node) -> perNodeMetricName
        private final Map<OperationState, Map<OperationState, Map<SocketAddress, String>>> nodeMetricKeyToMetricName = new HashMap<OperationState, Map<OperationState, Map<SocketAddress, String>>>();

        public String getOrCreateMetricName(OperationState prevState, OperationState newState) {
            Map<OperationState, String> newStateToMetricName = metricKeyToMetricName.get(prevState);
            if (newStateToMetricName == null) {
                newStateToMetricName = new HashMap<OperationState, String>();
                metricKeyToMetricName.put(prevState, newStateToMetricName);
            }

            String metricName = newStateToMetricName.get(newState);
            if (metricName == null) {
                metricName = "all-nodes-time-from-" + prevState + "-to-" + newState;
                newStateToMetricName.put(newState, metricName);
            }

            return metricName;
        }

        public String getOrCreateMetricName(OperationState prevState, OperationState newState, SocketAddress socketAddress) {
            Map<OperationState, Map<SocketAddress, String>> newStateAndNodeToMetricName = nodeMetricKeyToMetricName.get(prevState);
            if (newStateAndNodeToMetricName == null) {
                newStateAndNodeToMetricName = new HashMap<OperationState, Map<SocketAddress, String>>();
                nodeMetricKeyToMetricName.put(prevState, newStateAndNodeToMetricName);
            }

            Map<SocketAddress, String> nodeToMetricName = newStateAndNodeToMetricName.get(newState);
            if (nodeToMetricName == null) {
                nodeToMetricName = new HashMap<SocketAddress, String>();
                newStateAndNodeToMetricName.put(newState, nodeToMetricName);
            }

            String metricName = nodeToMetricName.get(socketAddress);
            if (metricName == null) {
                metricName = "node-" + socketAddress + "-time-from-" + prevState + "-to-" + newState;
                nodeToMetricName.put(socketAddress, metricName);
            }

            return metricName;
        }
    }
}