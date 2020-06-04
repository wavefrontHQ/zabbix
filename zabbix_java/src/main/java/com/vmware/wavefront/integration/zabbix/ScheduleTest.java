package com.vmware.wavefront.integration.zabbix;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleTest implements Runnable{

    public int counter = 0;
    public String hello = null;
    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.ScheduleTest.class);

    public ScheduleTest() {
        counter = 0;
        hello = "hello, world";
    }

    public static void main(String[] args) {
        logger.info("*************************************");
        logger.info("*         Zabbix Integration        *");
        logger.info("*************************************");

        Callable callabledelayedTask = new Callable() {
            public String call() throws Exception
            {
                return "GoodBye! See you at another invocation...";
            }
        };


        try {
            // create a scheduled thread pool to start thread with schedules
            ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(1, new DataThreadFactory("DataFetcher"));
            ScheduleTest test = new ScheduleTest();
            scheduledPool.scheduleAtFixedRate(test, 1, 5, TimeUnit.SECONDS);
            while(!scheduledPool.isTerminated()) {
            }
        } catch(Exception e) {
            logger.info("- ERROR -");
            logger.error(e.getMessage(), e);
        }
        logger.info("Schedule Pool Ended.");
    }

    public void run() {

        // run something - which will take 20 seconds
        try {
            logger.info(Thread.currentThread().getName() + " started... ");
            Thread.sleep(20000);
            counter++;
            logger.info("counter: " + counter);
            logger.info(Thread.currentThread().getName() + " done processing.");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
