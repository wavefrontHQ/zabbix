package com.vmware.wavefront.integration.zabbix;

/**
 * class that collects metrics gathered by zabbix integration.
 * simple metrics to accumulate ms spent on connection, query prep, query, iteration and
 * ultimately sending it to wavefront proxy.
 */
public class Metrics implements Cloneable {
    public long dbConnTime;                 // db connect time elapsed
    public long dbQueryPrepTime;            // query prepare time
    public long dbQueryExecTime;            // query execution time
    public long dbResultItrTime;            // total iteration time
    public long wfSendTime;                 // total send time
    public int sent;

    public Metrics() {
        reset();
    }

    public void reset() {
        dbConnTime = 0;
        dbQueryPrepTime = 0;
        dbQueryExecTime = 0;
        dbResultItrTime = 0;
        wfSendTime = 0;
        sent = 0;
    }

    public synchronized void addDbConnTime(long time) {
        dbConnTime += time;
    }

    public synchronized void addDbQueryPrepTime(long time) {
        dbQueryPrepTime += time;
    }

    public synchronized void addDbQueryExecTime(long time) {
        dbQueryExecTime += time;
    }

    public synchronized void addDbResultItrTime(long time) {
        dbResultItrTime += time;
    }

    public synchronized void addWfSendTime(long time) {
        wfSendTime += time;
    }

    public synchronized void addSent(int sent) {
        this.sent += sent;
    }

    public Object clone()throws CloneNotSupportedException {
        return super.clone();
    }
}
