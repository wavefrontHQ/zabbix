package com.vmware.wavefront.integration.zabbix;

import com.wavefront.integrations.Wavefront;

/**
 * data runner abstract class
 */
public abstract class DataRunner {
    protected String command;
    protected String getCommand() {
        return command;
    }
    protected Context ctx;
    protected Wavefront wavefront;

    public DataRunner(Context ctx) {
        try {
            this.ctx = (Context)ctx.clone();
        } catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
    }

    protected int randomWithRange(int min, int max)
    {
        int range = (max - min) + 1;
        return (int)(Math.random() * range) + min;
    }

    public synchronized void sendWavefront(String metric, double value) throws Exception {
        if(wavefront == null && ctx.metricsEnabled == true) {
            wavefront = ctx.getWavefront();
        }
        if(wavefront != null) {
            wavefront.send(ctx.prefix + metric, value, System.currentTimeMillis() / 1000l, ctx.integrationSource);
            wavefront.flush();
        }
    }

    public synchronized void sendWavefront(String metric, double value, long epochsec) throws Exception {
        if(wavefront == null && ctx.metricsEnabled == true) {
            wavefront = ctx.getWavefront();
        }
        if(wavefront != null) {
            wavefront.send(ctx.prefix + metric, value, epochsec, ctx.integrationSource);
            wavefront.flush();
        }
    }

    /**
     * close wavefront connection initialized
     */
    public void closeWavefront() {
        if(wavefront != null) {
            try {
                wavefront.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
