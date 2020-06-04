package com.vmware.wavefront.integration.zabbix;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zabbix integration which retrieves
 * rows from mysql database, converts them and send them to wavefront proxy.
 * it utilizes thread executor model to multi-thread the required metrics to
 * be collected and sent to zabbix.
 */
public class Zabbix {

    protected ContextMap ctxMap;
    static Logger logger;
    public Zabbix(ContextMap ctxMap) {
        this.ctxMap = ctxMap;
    }
    static {
        try {
            // load the log4j properties
            Properties p = new Properties();
            InputStream in = ClassLoader.getSystemResourceAsStream("log4j.properties");
            p.load(in);

            // if the external log4j.properties exists, load it over the default log4j
            String logConfigPath = "./log4j.properties";
            File logConfigFile = new File(logConfigPath);
            if(logConfigFile.exists()) {
                p.load(new FileInputStream(logConfigFile));
            }
            PropertyConfigurator.configure(p);
            logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.Zabbix.class);
        } catch (Exception e) {
           e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        ContextMap contextMap = new ContextMap();   // map will hold context for each tables.
        logger.info("*************************************");
        logger.info("*         Zabbix Integration        *");
        logger.info("*************************************");

        try {
            // setup micrometer registry to monitor JVM activities
            WavefrontConfig config = null;
            MeterRegistry registry = null;

            // if jvm metrics is enabled, register and use micrometer library to send JVM metrics
            if(ZabbixConfig.exists("jvm.metrics") && ZabbixConfig.getProperty("jvm.metrics").equalsIgnoreCase("true")) {
                config = new WavefrontConfig() {
                    @Override
                    public String uri() {
                        return "proxy://" + ZabbixConfig.getProperty("wavefront.proxy.host") + ":" + ZabbixConfig.getProperty("wavefront.proxy.port");
                    }

                    @Override
                    public String source() {
                        return ZabbixConfig.getProperty("integration.source");
                    }

                    @Override
                    public String get(String key) {
                        // defaults everything else
                        return null;
                    }

                    @Override
                    public Duration step() {
                        // 10 seconds reporting interval
                        return Duration.ofSeconds(10);
                    }

                    @Override
                    public String prefix() {
                        if (ZabbixConfig.exists("global.prefix")) {
                            return ZabbixConfig.getProperty("global.prefix") + ".integration";
                        }
                        return "integration";
                    }

                    @Override
                    public String globalPrefix() {
                        if (ZabbixConfig.exists("global.prefix")) {
                            return ZabbixConfig.getProperty("global.prefix") + ".integration";
                        }
                        return "integration";
                    }
                };

                // standard JVM metrics - using micrometer
                registry = new WavefrontMeterRegistry(config, Clock.SYSTEM);
                new ClassLoaderMetrics().bindTo(registry);
                new JvmMemoryMetrics().bindTo(registry);
                new JvmGcMetrics().bindTo(registry);
                new ProcessorMetrics().bindTo(registry);
                new JvmThreadMetrics().bindTo(registry);
                // now, the registry should be ready
            }

            // first, get the list of tables - defined in config.properties
            String tableList = ZabbixConfig.getProperty("zabbix.tables");
            StringTokenizer tokenizer = new StringTokenizer(tableList, ",");

            // initialize context map based on the configured tables.
            while(tokenizer.hasMoreElements()) {
                String table = tokenizer.nextToken().trim();
                Context context = new Context(table);
                contextMap.addContext(context);
            }

            // interval is defined in ms
            long interval = Long.parseLong(ZabbixConfig.getProperty("integration.fetch.interval"));

            ExecutorService executor = Executors.newCachedThreadPool(new DataThreadFactory("DataFetcher"));
            // setup the fetcher for each tables, and start off its process.
            List<Context> contexts = contextMap.getAllContexts();

            logger.info("--------- start ----------");

            for(Context ctx : contexts) {
                logger.info("starting Single Thread Data Fetcher " + ctx.getTableName());
                SingleThreadDataFetcher fetcher = new SingleThreadDataFetcher(ctx);
                executor.execute(fetcher);
            }
            executor.shutdown();
            while(!executor.isTerminated()) {
                logger.debug("Zabbix loop sleeping for " + interval + "ms");
                Thread.sleep(interval);
            }
        } catch(Exception e) {
            logger.info("- ERROR -");
            logger.error(e.getMessage(), e);
        }
        logger.info("Zabbix integration stopped.");
        System.exit(0);
    }
}
