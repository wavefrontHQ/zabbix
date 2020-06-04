package com.vmware.wavefront.integration.zabbix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LImitAdjuster is used by SingleThreadDataFetcher
 * when determining
 */
public class LimitAdjuster {
    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.LimitAdjuster.class);
    protected Context ctx;

    public LimitAdjuster(Context ctx) {
        this.ctx = ctx;
    }

    /**
     * adjust rate method - which adjusts the LIMIT based on the
     * sent rate and previous rate
     */
    public void adjustRate() {
        // if the limit has reached and rate seems to run higher, raise the limit.
        double avgRatio = ctx.getAvgRatio();
        // if the avg ratio of rate btw current and prev rate is higher
        // than 1.5, (50% increase or more), then consider increasing the Limits
        // if the avg ratio is dropped, (50% decrease or more), then consider decreasing the limits
        logger.debug("avgRatio is " + avgRatio + ", sent: " + ctx.getMetrics().sent + " and limit is " + ctx.getLimit());

        // only trigger this if the sent metrics exceeded the limit set
        if(ctx.getMetrics().sent > ctx.getLimit()) {
            // if avgRatio is calculated
            if (!Double.isNaN(avgRatio)) {
                if (avgRatio > 1.5d) {
                    // raise the limit if avgRatio exceeds 50%
                    ctx.setLimit(ctx.getLimit() + ctx.getIncremental());
                    logger.debug("Limit adjusted to: " + ctx.getLimit());
                } else if (avgRatio < 0.5d) {
                    // reduce the limit if avgRatio is down by 50%
                    if (ctx.getLimit() <= 0) {
                        ctx.setLimit(ctx.getLimit() - ctx.getIncremental());
                        logger.debug("Limit adjusted to: " + ctx.getLimit());
                    }
                } else {
                    // do not do anything if the avg ratio is between 50% ~ 150%
                    logger.debug("Limit not adjusted - avgRatio: " + avgRatio);
                }
            }
        }
    }
}
