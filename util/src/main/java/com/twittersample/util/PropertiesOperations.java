package com.twittersample.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesOperations {
    /**
     *
     * @return Properties
     * @throws IOException
     */
    public static Properties getProperties() throws IOException {
        InputStream is = PropertiesOperations.class.getClassLoader().getResourceAsStream("configuration.properties");
        Properties properties = new Properties();
        assert is != null;
        properties.load(is);

        return properties;
    }
}
