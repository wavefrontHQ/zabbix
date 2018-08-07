package com.vmware.wavefront.integration.zabbix;
import java.util.concurrent.ThreadFactory;

/**
 * Custom data thread factory responsible for generating
 * human-readable thread names
 */
public class DataThreadFactory implements ThreadFactory {

    int count;
    String prefix;
    public DataThreadFactory(String prefix) {
        count = 0;
        this.prefix = prefix;
    }

    public Thread newThread(Runnable r) {
        return new Thread(r, prefix + "-" + ++count);
    }
}
