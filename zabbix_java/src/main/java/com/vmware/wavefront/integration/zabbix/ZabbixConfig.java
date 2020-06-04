package com.vmware.wavefront.integration.zabbix;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * singleton config class that will load the config.properties and
 * provide configuration for zabbix integration to run properly
 */
public class ZabbixConfig {
    /**
     * automatically load the properties when
     * initialized. The config is singleton per jvm
     */
    protected static Properties config = new Properties();
    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.ZabbixConfig.class);
    static
    {
        // using the class loader, load the properties into config.
        try {
            // load the default config properties
            InputStream in = ClassLoader.getSystemResourceAsStream("config.properties");
            config.load(in);

            // if the external config file exists, load it over the default config.
            File configFile = new File("./config.properties");
            if(configFile.exists()) {
                in = new FileInputStream(configFile);
                config.load(in);
            }
            logger.debug(config.toString());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String name) {
        return config.getProperty(name);
    }

    public static String setProperty(String name, String value) {
        return (String)config.setProperty(name, value);
    }

    public static boolean exists(String name) {
        if(config.containsKey(name)) return true;
        else {
            if(config.getProperty(name) == null) return false;
            else {
                if(config.getProperty(name).trim().length() == 0) return false;
                else return true;
            }
        }
    }
}
