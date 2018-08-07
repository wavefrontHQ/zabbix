package com.vmware.wavefront.integration.zabbix;

import com.wavefront.integrations.Wavefront;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * single data fetcher is a simplified version of data fetcher.
 * this will run as a single thread, and run query to grab data, with certain
 * limits that can be elastically adjusted based on the rate of the
 * processing.
 *
 * the inverval time can also be adjusted, depending on the availability of the
 * data.
 */
public class SingleThreadDataFetcher extends DataRunner implements Fetcher {

    protected String tableName;

    private static Pattern punc_whitespace;
    private static Pattern trailing_dots;
    private static Pattern one_dot;

    protected LimitAdjuster limitAdjuster = null;

    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.SingleThreadDataFetcher.class);

    static {
        // regex patterns to reformat key to wavefront compatible metrics name
        punc_whitespace = Pattern.compile("[^\\w\\s\\-.]|\\s+");
        trailing_dots = Pattern.compile("\\.+$");
        one_dot = Pattern.compile("\\.+");
    }

    public SingleThreadDataFetcher(Context ctx) {
        super(ctx);
        limitAdjuster = new LimitAdjuster(this.ctx);
        tableName = ctx.getTableName();
        command = "[DataFetcher] " + tableName;
    }

    /**
     * run the main thread
     */
    public void run() {
        logger.info(Thread.currentThread().getName()+" Start. Command = " + getCommand());
        boolean exit = false;

        while(exit == false) {
            long startCmd = System.currentTimeMillis();
            try {
                // run the process iteration
                processCommand();
                long endCmd = System.currentTimeMillis();

                // calculate the rate of metrics processing.
                double sentRate = ((double) ctx.getMetrics().sent) / (double) (endCmd - startCmd);

                // once you get the rate, compare it with current rate. if rate is not specified yet,
                // don't do anything and just set the rate.
                double prevRate = ctx.getPointRate();

                logger.debug("sentRate: " + sentRate + " , prevRate: " + prevRate);
                if(prevRate != 0.0d) {
                    logger.debug("ratio: " + sentRate / prevRate);
                }
                logger.debug("avgRate: " + ctx.getAvgRatio());

                // depending on the rate, we should increase/decrease the LIMIT...
                // finally, update the rate in context
                ctx.setPointRate(sentRate);
                sendWavefront("integration." + tableName + ".sentRate", sentRate);
                sendWavefront("integration." + tableName + ".cycle.time", (double) (endCmd - startCmd));

                /****************************************
                 * Limit adjusting part
                 * where the initially set limit can be
                 * adjusted based on the sentRate and
                 * prevRate
                 */
                limitAdjuster.adjustRate();

                if (ctx.getMetrics().sent == 0) {
                    // need to sleep if no data has been found.
                    logger.info(ctx.getTableName() + ": no data has been found, sleeping for " + ctx.getInterval() + " ms...");
                    Thread.sleep(ctx.getInterval());
                } else if (ctx.getMetrics().sent < ctx.getLimit()) {
                    logger.info(ctx.getTableName() + ": data is less than limit:" + ctx.getMetrics().sent + " (current limit: " + ctx.getLimit() + ") sleeping for " + ctx.getInterval() + " ms...");
                    Thread.sleep(ctx.getInterval());
                }
            } catch(Exception e) {
                logger.error(ctx.getTableName() + " exiting, due to an error.", e);
                // break from the while loop
                exit = true;
            }
        }
    }

    private void processCommand() throws Exception {
        try {
            // mark the start of the cycle
            sendWavefront("integration." + tableName + ".cycle.mark", 1.0d);

            // for each iteration, we calculate DB's current time
            long currentTime = 0;
            {
                Connection conn = DBManager.getInstance().getConnection();
                try {
                    PreparedStatement pstmt = conn.prepareStatement("select unix_timestamp() clock");
                    ResultSet rset = pstmt.executeQuery();
                    if (rset.next()) {
                        long clock = rset.getLong("clock");
                        currentTime = clock;
                        rset.close();
                    }
                    pstmt.close();
                } catch (Exception e) {
                    throw e;
                } finally {
                    if (conn != null) try {
                        conn.close();
                    } catch (Exception e) {
                    }
                }
            }

            // check the last time in the context.
            long lastTime = ctx.getLatesttime();

            // if last time is 0, meaning it hasn't been initialized, we initialize using timestamp from DB.
            // this is found to be necessary, as query should be based on DB's time, not by client's time.
            if(lastTime == 0) {
                lastTime = currentTime;
                // check whether we have lag seconds set up.
                if(ZabbixConfig.exists("integration.fetch.lag")) {
                    int lag = Integer.parseInt(ZabbixConfig.getProperty("integration.fetch.lag"));
                    // push the lasttime back using specified lag, if the property exists.
                    lastTime -= lag;
                }
            }

            // record clock lag to wavefront, between last time and current time.
            this.sendWavefront("integration." + tableName + ".timeLag", currentTime - lastTime, currentTime);

            // reset metrics before continuing - as this would be a new iteration.
            ctx.getMetrics().reset();

            // proceed to extract metrics only when the mode is NOT profile only mode.
            if(ctx.getProfileOnly() == false) {

                // get the query - using limit.
                long dbconnStart = System.currentTimeMillis();
                Connection conn = DBManager.getInstance().getConnection();
                long dbconnEnd = System.currentTimeMillis();

                try {

                    long prepStart = System.currentTimeMillis();

                    String sql = SQLMySQL.getInstance().genQueryLimit(ctx.getTableName());
                    logger.debug("sql: " + sql + " lasttime:" + lastTime + " limit:" + ctx.getLimit());
                    PreparedStatement pstmt = conn.prepareStatement(sql);
                    pstmt.setLong(1, lastTime);
                    pstmt.setInt(2, ctx.getLimit());
                    if(ctx.getQueryTimeout() > 0) {
                        pstmt.setQueryTimeout(ctx.getQueryTimeout());
                    }
                    long prepEnd = System.currentTimeMillis();

                    long startQueryExec = System.currentTimeMillis();
                    ResultSet rset = pstmt.executeQuery();
                    long endQueryExec = System.currentTimeMillis();

                    // iterate through the loops
                    long largestClock = 0l;
                    int sent = 0;

                    long itrStart = System.currentTimeMillis();
                    Wavefront _wavefront = ctx.getWavefront();
                    while(rset.next()) {
                        // get clock
                        long clock = rset.getLong("clock");

                        // get other values
                        String value = rset.getString("value");
                        String host = reformatSource(rset.getString("host"));
                        String key = reformatMetricName(rset.getString("key_"));
                        if(clock > largestClock) largestClock = clock;

                        String prefix = ctx.getPrefix();
                        if(prefix != null) key = prefix + key;
                        String out = String.format("%s%s %s %d source=%s", prefix, key, value, clock, host);

                        // format and send it to wavefront.
                        if(ZabbixConfig.exists("integration.wavefront.send") && ZabbixConfig.getProperty("integration.wavefront.send").equalsIgnoreCase("true")) {
                            _wavefront.send(key, Double.parseDouble(value), clock, host);
                        }
                        else {
                            logger.debug(out);
                        }
                        sent++;
                    }
                    _wavefront.close();
                    long itrEnd = System.currentTimeMillis();

                    // refresh latest clock
                    if(largestClock != 0 || sent > 0) {
                        ctx.setLatesttime(largestClock);
                        logger.info("setting largest clock:" + largestClock);
                    }

                    ctx.getMetrics().addSent(sent);
                    ctx.getMetrics().addDbConnTime(dbconnEnd - dbconnStart);
                    ctx.getMetrics().addDbQueryPrepTime(prepEnd - prepStart);
                    ctx.getMetrics().addDbQueryExecTime(endQueryExec - startQueryExec);
                    ctx.getMetrics().addDbResultItrTime(itrEnd - itrStart);

                    // send out the key perf metrics
                    // db conn time - total ms took for establishing db connection
                    sendWavefront("integration." + tableName + ".dbconn.time", (double) ctx.getMetrics().dbConnTime);
                    // query prep time - running preprared statements for the query
                    sendWavefront("integration." + tableName + ".queryprep.time", (double) ctx.getMetrics().dbQueryPrepTime);
                    // query execution time - actual running of the query
                    sendWavefront("integration." + tableName + ".queryexec.time", (double) ctx.getMetrics().dbQueryExecTime);
                    // result set iteration time
                    sendWavefront("integration." + tableName + ".resultitr.time", (double) ctx.getMetrics().dbResultItrTime);
                    // sent point data
                    sendWavefront("integration." + tableName + ".sent", (double) ctx.getMetrics().sent);
                } catch(Exception e) {
                    throw e;
                } finally {
                    if(conn != null) try {conn.close();} catch(Exception e) {}
                }
            }
            // close wavefront proxy connection - required
            closeWavefront();

        } catch(Exception e) {
            throw e;
        }
    }

    /**
     * reformat string to fit for wavefront
     * source, timestamp, and value are in correct format.
     *
     * below are some of the examples of metrics resulting from zabbix
     * vfs.fs.inode[/etc/resolv.conf,pfree]
     * system.cpu.util[,user]
     * zabbix[wcache,values]
     * zabbix[process,alert manager,avg,busy]
     *
     * here's the strategy to 'reformat' them for wavefront
     * 1. replace punctuation and whitespace
     * 2. remove training dots
     * 3. just one dots (convert multiple dots to single dot)
     * 4. convert everything to lower case
     *
     * - code is derived from the original python script
     *
     * @param metricName - zabbix metric name
     * @return
     */
    protected String reformatMetricName(String metricName) {
        // replace punctuation and whitespace
        metricName = punc_whitespace.matcher(metricName).replaceAll(".");
        // remove trailing dots
        metricName = trailing_dots.matcher(metricName).replaceAll("");
        // remove dangling dot
        metricName = one_dot.matcher(metricName).replaceAll(".");
        return metricName.toLowerCase();
    }

    /**
     * similar with reformat metric name, but
     * with host names
     * @param source
     * @return
     */
    protected String reformatSource(String source) {
        source = punc_whitespace.matcher(source).replaceAll(".");
        source = source.replace('_', '.');
        return source;
    }

    @Override
    public String toString(){
        return this.command;
    }
}
