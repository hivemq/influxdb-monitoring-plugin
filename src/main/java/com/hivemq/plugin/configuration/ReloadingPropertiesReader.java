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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.hivemq.spi.annotations.NotNull;
import com.hivemq.spi.config.SystemInformation;
import com.hivemq.spi.services.PluginExecutorService;
import com.hivemq.spi.services.configuration.ValueChangedCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Christoph Schäbel
 */
abstract class ReloadingPropertiesReader {

    private static final Logger log = LoggerFactory.getLogger(ReloadingPropertiesReader.class);

    private final PluginExecutorService pluginExecutorService;
    private final SystemInformation systemInformation;
    Properties properties;
    private File file;
    private Map<String, List<ValueChangedCallback<String>>> callbacks = Maps.newHashMap();


    private final static String ENV_VAR_HOST = "HIVEMQ_INFLUX_DB_PLUGIN_HOST";
    private final static String ENV_VAR_PORT = "HIVEMQ_INFLUX_DB_PLUGIN_PORT";
    private final static String ENV_VAR_AUTH = "HIVEMQ_INFLUX_DB_PLUGIN_AUTH";

    private final static String ENV_VAR_PROOCOL = "HIVEMQ_INFLUX_DB_PLUGIN_PROTOCOL";
    private final static String ENV_VAR_MODE = "HIVEMQ_INFLUX_DB_PLUGIN_MODE";
    private final static String ENV_VAR_PREFIX = "HIVEMQ_INFLUX_DB_PLUGIN_PREFIX";
    private final static String ENV_VAR_DATABASE = "HIVEMQ_INFLUX_DB_PLUGIN_DATABASE";
    private final static String ENV_VAR_REPORTING_INTERVAL = "HIVEMQ_INFLUX_DB_PLUGIN_REPORTING_INTERVAL";
    private final static String ENV_VAR_CONNECTION_TIMEOUT = "HIVEMQ_INFLUX_DB_PLUGIN_CONNECTION_TIMEOUT";
    private final static String ENV_VAR_TAGS = "HIVEMQ_INFLUX_DB_PLUGIN_TAGS";


    ReloadingPropertiesReader(final PluginExecutorService pluginExecutorService,
                              final SystemInformation systemInformation) {
        this.pluginExecutorService = pluginExecutorService;
        this.systemInformation = systemInformation;
    }



    //do not put a postconstruct here, else this method gets called twice
    public void postConstruct() {
        file = new File(systemInformation.getConfigFolder() + "/" + getFilename());
        properties = new Properties();
        try  {
            properties = loadProperties();

        } catch (IOException e) {
            log.error("Not able to load configuration file {}", file.getAbsolutePath());
        }

        pluginExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reload();
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    String getProperty(final String key) {
        if (properties == null) {
            return null;
        }
        return properties.getProperty(key);
    }

    /**
     * Loads Properties from configuration file, it does NOT set it but return it
     * @return loaded Properties
     * @throws IOException this is thrown and not caught inside the method because the log level in error case is different on starting the plugin than on reload
     */
    private Properties loadProperties() throws IOException{
        try (final FileReader in = new FileReader(file)) {
            final Properties props = new Properties();
            props.load(in);
            overwritePropertiesWithEnvVar(props);
            return props;
        }


    }

    protected Properties overwritePropertiesWithEnvVar(@NotNull Properties props){
        checkNotNull(props, "Props must not be null");


        if(System.getenv(ENV_VAR_HOST)!= null){
            props.put(InfluxDbConfiguration.HOST, System.getenv(ENV_VAR_HOST));
        }

        if(System.getenv(ENV_VAR_PORT)!= null){
            props.put(InfluxDbConfiguration.PORT, System.getenv(ENV_VAR_PORT));
        }

        if(System.getenv(ENV_VAR_AUTH)!= null){
            props.put(InfluxDbConfiguration.AUTH, System.getenv(ENV_VAR_AUTH));
        }

        if(System.getenv(ENV_VAR_MODE)!=null){
            props.put(InfluxDbConfiguration.MODE, System.getenv(ENV_VAR_MODE));
        }

        if(System.getenv(ENV_VAR_PROOCOL)!=null){
            props.put(InfluxDbConfiguration.PROTOCOL, System.getenv(ENV_VAR_PROOCOL));
        }

        if(System.getenv(ENV_VAR_PREFIX)!=null){
            props.put(InfluxDbConfiguration.PREFIX, System.getenv(ENV_VAR_PREFIX));
        }

        if(System.getenv(ENV_VAR_DATABASE)!=null){
            props.put(InfluxDbConfiguration.DATABASE, System.getenv(ENV_VAR_DATABASE));
        }

        if(System.getenv(ENV_VAR_REPORTING_INTERVAL)!=null){
            props.put(InfluxDbConfiguration.REPORTING_INTERVAL, System.getenv(ENV_VAR_REPORTING_INTERVAL));
        }

        if(System.getenv(ENV_VAR_CONNECTION_TIMEOUT)!=null){
            props.put(InfluxDbConfiguration.CONNECT_TIMEOUT, System.getenv(ENV_VAR_CONNECTION_TIMEOUT));
        }

        if(System.getenv(ENV_VAR_TAGS)!=null){
            props.put(InfluxDbConfiguration.TAGS, System.getenv(ENV_VAR_TAGS));
        }





        return props;
    }




    @NotNull
    public abstract String getFilename();

    /**
     * Reloads the specified .properties file
     */
    @VisibleForTesting
    void reload() {

        Map<String, String> oldValues = getCurrentValues();

        try{
            properties = loadProperties();
            Map<String, String> newValues = getCurrentValues();
            logChanges(oldValues, newValues);
        } catch (IOException e) {
            log.debug("Not able to reload configuration file {}", this.file.getAbsolutePath());
        }
    }

    void addCallback(@NotNull final String propertyName,@NotNull final ValueChangedCallback<String> changedCallback) {
      checkNotNull(propertyName, "Property name must not be null");
      checkNotNull(changedCallback, "Changed callback must not be null");


        if (!callbacks.containsKey(propertyName)) {
            callbacks.put(propertyName, Lists.<ValueChangedCallback<String>>newArrayList());
        }

        callbacks.get(propertyName).add(changedCallback);
    }

    private Map<String, String> getCurrentValues() {
        Map<String, String> values = Maps.newHashMap();
        for (String key : properties.stringPropertyNames()) {
            values.put(key, properties.getProperty(key));
        }
        return values;
    }

    private void logChanges(final Map<String, String> oldValues, final Map<String, String> newValues) {
        final MapDifference<String, String> difference = Maps.difference(oldValues, newValues);

        for (Map.Entry<String, MapDifference.ValueDifference<String>> stringValueDifferenceEntry : difference.entriesDiffering().entrySet()) {
            log.debug("Plugin configuration {} changed from {} to {}",
                    stringValueDifferenceEntry.getKey(), stringValueDifferenceEntry.getValue().leftValue(),
                    stringValueDifferenceEntry.getValue().rightValue());

            if (callbacks.containsKey(stringValueDifferenceEntry.getKey())) {
                for (ValueChangedCallback<String> callback : callbacks.get(stringValueDifferenceEntry.getKey())) {
                    callback.valueChanged(stringValueDifferenceEntry.getValue().rightValue());
                }
            }
        }

        for (Map.Entry<String, String> stringStringEntry : difference.entriesOnlyOnLeft().entrySet()) {
            log.debug("Plugin configuration {} removed", stringStringEntry.getKey(), stringStringEntry.getValue());
            if (callbacks.containsKey(stringStringEntry.getKey())) {
                for (ValueChangedCallback<String> callback : callbacks.get(stringStringEntry.getKey())) {
                    callback.valueChanged(properties.getProperty(stringStringEntry.getValue()));
                }
            }
        }

        for (Map.Entry<String, String> stringStringEntry : difference.entriesOnlyOnRight().entrySet()) {
            log.debug("Plugin configuration {} added: {}", stringStringEntry.getKey(), stringStringEntry.getValue());
            if (callbacks.containsKey(stringStringEntry.getKey())) {
                for (ValueChangedCallback<String> callback : callbacks.get(stringStringEntry.getKey())) {
                    callback.valueChanged(stringStringEntry.getValue());
                }
            }
        }

    }

    @NotNull
    Properties getProperties() {
        return properties;
    }


}