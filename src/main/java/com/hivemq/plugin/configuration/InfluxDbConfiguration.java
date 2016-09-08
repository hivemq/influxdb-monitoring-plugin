/*
 * Copyright 2015 dc-square GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.plugin.configuration;

import com.hivemq.spi.annotations.NotNull;
import com.hivemq.spi.annotations.Nullable;
import com.hivemq.spi.config.SystemInformation;
import com.hivemq.spi.services.PluginExecutorService;
import com.hivemq.spi.services.configuration.ValueChangedCallback;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This reads a property file and provides some utility methods for working with {@link Properties}
 *
 * @author Christoph Schäbel
 */
@Singleton
public class InfluxDbConfiguration extends ReloadingPropertiesReader {

    private static final Logger log = LoggerFactory.getLogger(InfluxDbConfiguration.class);
    public static final HashMap<String, String> EMPTY_MAP = new HashMap<>();

    private RestartListener listener;

    @Inject
    public InfluxDbConfiguration(final PluginExecutorService pluginExecutorService,
                                 final SystemInformation systemInformation) {
        super(pluginExecutorService, systemInformation);

        final ValueChangedCallback<String> callback = new ValueChangedCallback<String>() {
            @Override
            public void valueChanged(final String newValue) {
                if (listener != null) {
                    listener.restart();
                }
            }
        };

        addCallback("mode", callback);
        addCallback("host", callback);
        addCallback("port", callback);
        addCallback("protocol", callback);
        addCallback("reportingInterval", callback);
        addCallback("prefix", callback);
        addCallback("database", callback);
        addCallback("auth", callback);
        addCallback("connectTimeout", callback);
        addCallback("tags", callback);
    }

    @NotNull
    public String mode() {
        final String mode = getProperty("mode");
        if (mode == null) {
            log.warn("No mode configured for InfluxDb, using default: HTTP");
            return "http";
        }
        return mode;
    }

    @NotNull
    public String host() {
        final String host = getProperty("host");
        if (host == null) {
            log.warn("No host configured for InfluxDb, using default: localhost");
            return "localhost";
        }
        return properties.getProperty("host");
    }

    public int port() {
        final String portProp = properties.getProperty("port");
        if (portProp == null) {
            log.warn("No port configured for InfluxDb, using default: 8086");
            return 8086;
        }
        try {
            return Integer.parseInt(portProp);
        } catch (NumberFormatException e) {
            log.error("Invalid format {} for InfluxDB property port, using default: 8086", portProp);
            return 8086;
        }
    }

    @NotNull
    public String protocol() {
        final String protocol = getProperty("protocol");
        if (protocol == null) {
            if (mode().equals("http")) {
                log.warn("No protocol configured for InfluxDb, using default: http");
            }
            return "http";
        }
        return protocol;
    }

    public int reportingInterval() {
        final String reportingInterval = properties.getProperty("reportingInterval");
        if (reportingInterval == null) {
            log.warn("ReportingInterval property for InfluxDb not configured, using default: 1");
            return 1;
        }
        try {
            return Integer.parseInt(reportingInterval);
        } catch (NumberFormatException e) {
            log.error("Invalid format {} for InfluxDB property reportingInterval, using default: 1", reportingInterval);
            return 1;
        }
    }

    @NotNull
    public String prefix() {
        final String prefix = getProperty("prefix");
        if (prefix == null) {
            return "";
        }
        return prefix;
    }

    @NotNull
    public String database() {
        final String database = getProperty("database");
        if (database == null) {
            log.warn("No database configured for InfluxDb, using default: hivemq");
            return "hivemq";
        }
        return database;
    }

    public int connectTimeout() {
        final String connectTimeout = properties.getProperty("connectTimeout");
        if (connectTimeout == null) {
            log.warn("No connectTimeout configured for InfluxDb, using default: 5000");
            return 5000;
        }
        try {
            return Integer.parseInt(connectTimeout);
        } catch (NumberFormatException e) {
            log.error("Invalid format {} for InfluxDB property connectTimeout, using default: 5000", connectTimeout);
            return 5000;
        }
    }

    @Nullable
    public String auth() {
        return getProperty("auth");
    }

    @Override
    public String getFilename() {
        return "influxdb.properties";
    }

    public void setRestartListener(final RestartListener listener) {
        this.listener = listener;
    }

    @NotNull
    public Map<String, String> tags() {

        final String tags = getProperty("tags");
        if (tags == null) {
            return EMPTY_MAP;
        }

        final String[] split = StringUtils.splitPreserveAllTokens(tags, ";");

        if (split.length < 1) {
            return EMPTY_MAP;
        }

        final HashMap<String, String> tagMap = new HashMap<>();

        for (String tag : split) {
            final String[] tagPair = StringUtils.split(tag, "=");
            if (tagPair.length != 2 || tagPair[0].length() < 1 || tagPair[1].length() < 1) {
                log.warn("Invalid tag format {} for InfluxDB", tag);
                continue;
            }

            tagMap.put(tagPair[0], tagPair[1]);
        }

        return tagMap;
    }

    public interface RestartListener {
        void restart();
    }
}