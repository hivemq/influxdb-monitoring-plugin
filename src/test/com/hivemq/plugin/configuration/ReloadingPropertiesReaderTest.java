package com.hivemq.plugin.configuration;


import com.hivemq.spi.config.SystemInformation;
import com.hivemq.spi.services.PluginExecutorService;
import com.hivemq.spi.services.configuration.ValueChangedCallback;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author Christoph Schäbel
 */
public class ReloadingPropertiesReaderTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Mock
    PluginExecutorService pluginExecutorService;

    @Mock
     SystemInformation systemInformation;

    @Mock
    ValueChangedCallback<String> changedCallback;

    private ReloadingPropertiesReader reader;

    private File tempFile;

    @Captor
    ArgumentCaptor<String> captor;


    @Before
    public void before() throws Exception {

        MockitoAnnotations.initMocks(this);


        tempFile = tmpFolder.newFile(RandomStringUtils.randomAlphabetic(10) + ".properties");

        final Properties properties = new Properties();
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");
        properties.setProperty("key3", "value3");
        properties.store(new FileOutputStream(tempFile), "");

        when(systemInformation.getConfigFolder()).thenReturn(new File(tempFile.getAbsolutePath()));
        reader = new TestReloadingPropertiesReader(pluginExecutorService, systemInformation, "");

    }

    @Test
    public void test_no_properties_file() throws Exception {

        reader = new TestReloadingPropertiesReader(pluginExecutorService, systemInformation, "notexisting");

        reader.postConstruct();

        assertNotNull(reader.getProperties());
    }

    @Test
    public void test_file_removed() throws Exception {

        reader.postConstruct();

        assertEquals("value1", reader.getProperties().get("key1"));

        assertTrue(tempFile.delete());

        reader.reload();

        assertEquals("value1", reader.getProperties().get("key1"));

    }

    @Test
    public void test_post_construct() throws Exception {

        reader.postConstruct();

        verify(pluginExecutorService, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        assertNotNull(reader.getProperties());
    }

    @Test
    public void test_reload() throws Exception {

        reader.postConstruct();

        assertEquals("value1", reader.getProperties().get("key1"));

        final Properties properties = new Properties();
        properties.setProperty("key1", "othervalue1");
        properties.store(new FileOutputStream(tempFile), "");

        reader.reload();

        assertEquals("othervalue1", reader.getProperties().get("key1"));
    }

    @Test
    public void test_addCallback() throws Exception {

        reader.postConstruct();

        assertEquals("value2", reader.getProperties().get("key2"));

        final CountDownLatch latch = new CountDownLatch(1);
        final String[] callbackValue = new String[1];

        reader.addCallback("key2", new ValueChangedCallback<String>() {
            @Override
            public void valueChanged(final String newValue) {
                callbackValue[0] = newValue;
                latch.countDown();
            }
        });

        final Properties properties = new Properties();
        properties.setProperty("key2", "othervalue2");
        properties.store(new FileOutputStream(tempFile), "");

        reader.reload();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("othervalue2", callbackValue[0]);
        assertEquals("othervalue2", reader.getProperties().get("key2"));
    }

    @Test
    public void test_getProperties() throws Exception {

        reader.postConstruct();

        assertEquals("value1", reader.getProperties().get("key1"));
        assertEquals("value2", reader.getProperties().get("key2"));
        assertEquals("value3", reader.getProperties().get("key3"));
    }


    @Test
    public void test_callbacks_add() throws Exception {

        reader.addCallback("test",changedCallback);
        reader.postConstruct();

        try(FileWriter out = new FileWriter(tempFile)){
            out.write("test=123\n");
            out.flush();
        }
        reader.reload();
        Mockito.verify(changedCallback).valueChanged(captor.capture());
        Assert.assertTrue(captor.getValue().equals("123"));
    }

    @Test
    public void test_callbacks_remove() throws Exception {
        try(FileWriter out = new FileWriter(tempFile)){
            out.write("test=123\n");
            out.flush();
        }
        reader.addCallback("test",changedCallback);
        reader.postConstruct();

        try(FileWriter out = new FileWriter(tempFile)){
            out.write("#test=123\n");
            out.flush();
        }
        reader.reload();
        Mockito.verify(changedCallback).valueChanged(captor.capture());
        Assert.assertNull(captor.getValue());

    }

    private static class TestReloadingPropertiesReader extends ReloadingPropertiesReader {

        private final String filename;

        public TestReloadingPropertiesReader(final PluginExecutorService pluginExecutorService, final SystemInformation systemInformation, final String filename) {
            super(pluginExecutorService, systemInformation);
            this.filename = filename;
        }

        @Override
        public String getFilename() {
            return filename;
        }
    }





}