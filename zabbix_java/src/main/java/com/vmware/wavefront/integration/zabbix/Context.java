package com.vmware.wavefront.integration.zabbix;

import com.wavefront.integrations.Wavefront;

import java.io.*;

/**
 * main context class to store the context of the current
 * integration works - these are shared by data fetcher and sender to help them
 * get access to information like table name and number of metrics, as well as
 * last timestamp. The context also keeps track of latest timestamp by storing them
 * into file and continuously synchronizing it.
 */
public class Context implements Cloneable{

    protected String tableName;
    protected long latesttime;
    protected File timestampFile;
    protected boolean metricsEnabled;
    protected String integrationSource;
    protected String wfhost;
    protected String wfport;
    protected String prefix;
    protected int numberOfMetrics = 0;
    protected Metrics metrics;
    protected int queryTimeout;

    protected long intervalCount = 0l;
    // sum of point Rate ratio (current / prev)
    protected double sumRatio = 0d;

    protected double getAvgRatio() {
        // returns sum of ratio in average
        return sumRatio / intervalCount;
    }

    public int getIncremental() {
        return incremental;
    }

    public void setIncremental(int incremental) {
        this.incremental = incremental;
    }

    protected int incremental;

    public double getPointRate() {
        return pointRate;
    }

    public void setPointRate(double pointRate) {
        // for calculating sum of ratio
        if(this.pointRate != 0.0d && pointRate != 0.0d) {
            sumRatio += (pointRate / this.pointRate);
            intervalCount++;
        }
        this.pointRate = pointRate;
    }

    double pointRate;

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    protected long interval;

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    protected int limit = 0;
    boolean profileOnly = false;

    public int getLagsec() {
        return lagsec;
    }

    public void setLagsec(int lagsec) {
        this.lagsec = lagsec;
    }

    protected int lagsec = 0;

    public int getNumberOfSenders() {
        return numberOfSenders;
    }

    public void setNumberOfSenders(int numberOfSenders) {
        this.numberOfSenders = numberOfSenders;
    }

    protected int numberOfSenders = 0;

    public Metrics getMetrics() {
        return metrics;
    }

    public String getWfhost() {
        return wfhost;
    }

    public String getWfport() {
        return wfport;
    }

    public String getPrefix() {
        return prefix;
    }

    /**
     * main constructor
     * @param tableName
     *
     * constructor sets up context using given table name.
     * also sets up initial starting time, either from the existing file, or if not found,
     * from the initial timestamp minus the interval.
     */
    public Context(String tableName) {
        metrics = new Metrics();
        this.tableName = tableName;
        this.metricsEnabled = false;
        this.intervalCount = 0l;
        this.sumRatio = 0d;

        if(ZabbixConfig.exists("integration.metrics") && ZabbixConfig.getProperty("integration.metrics").equalsIgnoreCase("true")) {
            this.metricsEnabled = true;
        }
        integrationSource = ZabbixConfig.getProperty("integration.source");
        wfhost = ZabbixConfig.getProperty("wavefront.proxy.host");
        wfport = ZabbixConfig.getProperty("wavefront.proxy.port");
        prefix = ZabbixConfig.getProperty("global.prefix");
        profileOnly = Boolean.valueOf(ZabbixConfig.getProperty("integration.profile.only")).booleanValue();
        lagsec = Integer.parseInt(ZabbixConfig.getProperty("integration.fetch.lag"));
        limit = Integer.parseInt(ZabbixConfig.getProperty("integration.batch.size"));
        queryTimeout = Integer.parseInt(ZabbixConfig.getProperty("integration.query.timeout"));
        interval = Long.parseLong(ZabbixConfig.getProperty("integration.fetch.interval"));
        pointRate = 0.0d;
        incremental = Integer.parseInt(ZabbixConfig.getProperty("integration.fetch.incremental"));
        latesttime = 0;

        if(prefix != null) {
            if(prefix.trim().length() == 0) prefix = "";
            else {
                prefix = prefix.trim() + ".";
            }
        } else {
            prefix = "";
        }
    }

    public Wavefront getWavefront() {
        return new Wavefront(wfhost, Integer.parseInt(wfport));
    }

    public String getFileName() {
        return tableName + ".hist";
    }

    public String getTableName() {
        return tableName;
    }

    public void refreshTimestamp() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(timestampFile));
            writer.write(Long.toString(getLatesttime()));
            writer.flush();
            writer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getIntegrationSource() {
        return integrationSource;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getLatesttime() {
        return latesttime;
    }

    public void setLatesttime(long latesttime) {
        this.latesttime = latesttime;
    }

    public File getTimestampFile() {
        return timestampFile;
    }

    public void setTimestampFile(File timestampFile) {
        this.timestampFile = timestampFile;
    }

    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    public void setMetricsNumber(int number) {
        numberOfMetrics = number;
    }

    public int getMetricsNumber() {
        return numberOfMetrics;
    }

    public boolean getProfileOnly() {
        return profileOnly;
    }

    public Object clone()throws CloneNotSupportedException {
        return super.clone();
    }
}
