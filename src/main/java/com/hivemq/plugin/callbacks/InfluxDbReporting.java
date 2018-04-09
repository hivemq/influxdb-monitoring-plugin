package com.hivemq.plugin.callbacks;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.collect.Sets;
import com.hivemq.plugin.configuration.InfluxDbConfiguration;
import com.hivemq.spi.callback.CallbackPriority;
import com.hivemq.spi.callback.events.broker.OnBrokerStart;
import com.hivemq.spi.callback.events.broker.OnBrokerStop;
import com.hivemq.spi.callback.exception.BrokerUnableToStartException;
import com.izettle.metrics.influxdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Christoph Schäbel
 */
public class InfluxDbReporting implements OnBrokerStart, OnBrokerStop {

    private static final Logger log = LoggerFactory.getLogger(InfluxDbReporting.class);
    public static final HashSet<String> METER_FIELDS = Sets.newHashSet("count", "m1_rate", "m5_rate", "m15_rate", "mean_rate");
    public static final HashSet<String> TIMER_FIELDS = Sets.newHashSet("count", "min", "max", "mean", "stddev", "p50", "p75", "p95", "p98", "p99", "p999", "m1_rate", "m5_rate", "m15_rate", "mean_rate");

    private final MetricRegistry metricRegistry;
    private final InfluxDbConfiguration configuration;
    private InfluxDbSender sender;
    private ScheduledReporter reporter;

    @Inject
    public InfluxDbReporting(final MetricRegistry metricRegistry,
                             final InfluxDbConfiguration configuration) {
        this.metricRegistry = metricRegistry;
        this.configuration = configuration;
    }

    @Override
    public void onBrokerStart() throws BrokerUnableToStartException {

        startReporting();
        addRestartListener();
    }

    @Override
    public void onBrokerStop() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    @Override
    public int priority() {
        return CallbackPriority.MEDIUM;
    }


    private void addRestartListener() {

        configuration.setRestartListener(new InfluxDbConfiguration.RestartListener() {
            @Override
            public void restart() {
                reporter.close();
                startReporting();
            }
        });

    }

    private void startReporting() {
        setupSender();
        setupReporter();

        reporter.start(configuration.reportingInterval(), TimeUnit.SECONDS);
    }

    private void setupReporter() {

        final Map<String, String> tags = configuration.tags();

        reporter = InfluxDbReporter.forRegistry(metricRegistry)
                .withTags(tags)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .groupGauges(false)
                .skipIdleMetrics(false)
                .includeMeterFields(METER_FIELDS)
                .includeTimerFields(TIMER_FIELDS)
                .build(sender);
    }

    private void setupSender() {
        final String host = configuration.host();
        final int port = configuration.port();
        final String protocol = configuration.protocol();
        final String database = configuration.database();
        final String auth = configuration.auth();
        final int connectTimeout = configuration.connectTimeout();
        final String prefix = configuration.prefix();

        try {
            switch (configuration.mode()) {
                case "http":
                    log.info("Creating InfluxDB HTTP sender for server {}:{} and database {}", host, port, database);
                    sender = new InfluxDbHttpSender(protocol, host, port, database, auth, TimeUnit.SECONDS, connectTimeout, connectTimeout, prefix);
                    break;
                case "tcp":
                    log.info("Creating InfluxDB TCP sender for server {}:{} and database {}", host, port, database);
                    sender = new InfluxDbTcpSender(host, port, connectTimeout, database, TimeUnit.SECONDS, prefix);
                    break;
                case "udp":
                    log.info("Creating InfluxDB UDP sender for server {}:{} and database {}", host, port, database);
                    sender = new InfluxDbUdpSender(host, port, connectTimeout, database, TimeUnit.SECONDS, prefix);
                    break;

            }
        } catch (Exception ex) {
            log.error("Not able to start InfluxDB sender, please check your configuration: {}", ex.getMessage());
            log.debug("Original Exception: ", ex);
        }

    }


}
