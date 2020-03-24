package net.spy.memcached.ops;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.metrics.MetricCollector;

/**
 * An {@link OperationStateChangeObserver} implementation for monitoring purposes.
 * */
public class MonitoringOperationStateChangeObserver implements OperationStateChangeObserver {

    private final CachedMetricNames cachedMetricNames;
    private final MetricCollector metricCollector;

    MonitoringOperationStateChangeObserver(MetricCollector metricCollector) {
        this.metricCollector = metricCollector;
        this.cachedMetricNames = new CachedMetricNames(metricCollector);
    }

    public void stateChanged(Operation operation, OperationState prevState, OperationState currentState, long timeFromPrevToCurrentStateMicros) {
        String metricName = cachedMetricNames.getOrCreateMetricName(prevState, currentState);
        String perNodeMetricName = cachedMetricNames.getOrCreateMetricName(prevState, currentState, operation.getHandlingNode().getSocketAddress());

        metricCollector.updateHistogram(metricName, (int) timeFromPrevToCurrentStateMicros);
        metricCollector.updateHistogram(perNodeMetricName, (int) timeFromPrevToCurrentStateMicros);
    }

    /**
     * A simple cache for the metrics names, to avoid creating them per state-change per operation
     * */
    private static class CachedMetricNames {

        // (prevState, newState) -> metricName
        private final Map<OperationState, Map<OperationState, String>> metricKeyToMetricName = new HashMap<OperationState, Map<OperationState, String>>();
        // (prevState, newState, node) -> perNodeMetricName
        private final Map<OperationState, Map<OperationState, Map<SocketAddress, String>>> nodeMetricKeyToMetricName = new HashMap<OperationState, Map<OperationState, Map<SocketAddress, String>>>();
        private final MetricCollector metricCollector;

        public CachedMetricNames(MetricCollector metricCollector) {
            this.metricCollector = metricCollector;
        }

        String getOrCreateMetricName(OperationState prevState, OperationState newState) {
            Map<OperationState, String> newStateToMetricName = metricKeyToMetricName.get(prevState);
            if (newStateToMetricName == null) {
                newStateToMetricName = new HashMap<OperationState, String>();
                metricKeyToMetricName.put(prevState, newStateToMetricName);
            }

            String metricName = newStateToMetricName.get(newState);
            if (metricName == null) {
                metricName = String.format("overall-time-from-%s-to-%s", prevState, newState);
                metricCollector.addHistogram(metricName);
                newStateToMetricName.put(newState, metricName);
            }

            return metricName;
        }

        String getOrCreateMetricName(OperationState prevState, OperationState newState, SocketAddress socketAddress) {
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
                metricName = String.format("node-%s-time-from-%s-to-%s", socketAddress, prevState, newState);
                metricCollector.addHistogram(metricName);
                nodeToMetricName.put(socketAddress, metricName);
            }

            return metricName;
        }
    }
}