package com.vmware.wavefront.integration.zabbix;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * data mapper fetcher class which is a runnable thread that
 * 1. calculates the total number of metrics to fetch
 * 2. determines number of 'sender' to spawn and run in multiple threads
 * 3. starts out the thread to query and send metrics to wavefront proxy
 *
 * -------- NOTE --------
 * THIS CLASS IS DEPRECATED, NO LONGER BEING USED.
 * THIS CLASS MAY CONTAIN BUGS
 */
public class DataFetcher extends DataRunner implements Fetcher {

    protected String tableName;
    protected int batchSize;
    protected long start = 0;
    protected long end = 0;

    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.DataFetcher.class);

    public DataFetcher(Context ctx, long start, long end) {
        this(ctx, start, end, Integer.parseInt(ZabbixConfig.getProperty("integration.batch.size")));
    }

    public DataFetcher(Context ctx, long start, long end, int batchSize) {
        super(ctx);
        this.tableName = ctx.getTableName();
        this.batchSize = batchSize;
        this.start = start - ctx.getLagsec();
        this.end = end - ctx.getLagsec();
        command = "[DataFetcher] " + tableName + " " + start + " ~ " + end + "  batchsize:" + batchSize;
    }

    public Context getContext() {
        return ctx;
    }

    public String getTable() {
        return tableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void run() {
        logger.debug(Thread.currentThread().getName()+" Start. Command = " + getCommand());
        processCommand();
        logger.debug(Thread.currentThread().getName()+" End.");
    }

    private void processCommand() {
        try {
            // mark the start of the cycle
            sendWavefront("integration." + tableName + ".cycle.mark", 1.0d);
            long startNum = System.currentTimeMillis();
            // int metrics = getNumMetrics(start, end);

            // simulate random delay between 1 ~ 10 second
            /*
            long delay = randomWithRange(1,20) * 1000l;
            logger.debug(Thread.currentThread().getName() + " delay " + delay + "ms..");
            Thread.currentThread().sleep(delay);
            */

            // long endNum = System.currentTimeMillis();
            // endNum = endNum - startNum;
            // sendWavefront("integration." + tableName + ".getnum.time", (double)endNum);
            // sendWavefront("integration." + tableName + ".metrics.size", (double)metrics);

            logger.debug("["+ tableName +"] fetch at " + start + "~ with batch size: " + getBatchSize());

            int batchSize = getBatchSize();
            // int batch = (metrics / batchSize) + 1;
            // explicitly restrict batch number
            int batch = 1;
            ctx.setNumberOfSenders(batch);
            sendWavefront("integration." + tableName + ".thread.size", (double)batch);

            // reset metrics before continuing - as this would be a new iteration.
            ctx.getMetrics().reset();

            // proceed to extract metrics only when the mode is NOT profile only mode.
            if(ctx.getProfileOnly() == false) {
                ExecutorService executor = Executors.newFixedThreadPool(batch, new DataThreadFactory("DataSender-" + tableName + "-" + start));
                for (int i = 0; i < batch; i++) {
                    Runnable worker = new DataSender(this, start, end, i * batchSize, (i * batchSize) + (batchSize - 1));
                    executor.execute(worker);
                }
                long startCmd = System.currentTimeMillis();
                executor.shutdown();
                while (!executor.isTerminated()) {
                    Thread.currentThread().sleep(1000l);
                }
                long endCmd = System.currentTimeMillis();
                endCmd = endCmd - startCmd;
                sendWavefront("integration." + tableName + ".execute.time", (double) endCmd);

                // send out the key perf metrics
                // db conn time - total ms took for establishing db connection
                sendWavefront("integration." + tableName + ".dbconn.time", (double) ctx.getMetrics().dbConnTime);
                // query prep time - running preprared statements for the query
                sendWavefront("integration." + tableName + ".queryprep.time", (double) ctx.getMetrics().dbQueryPrepTime);
                // query execution time - actual running of the query
                sendWavefront("integration." + tableName + ".queryexec.time", (double) ctx.getMetrics().dbQueryExecTime);
                // result set iteration time
                sendWavefront("integration." + tableName + ".resultitr.time", (double) ctx.getMetrics().dbResultItrTime);
                // within the iteration, time it took for generating wf data and sending it.
                sendWavefront("integration." + tableName + ".wfsend.time", (double) ctx.getMetrics().wfSendTime);
                // sent point data
                sendWavefront("integration." + tableName + ".sent", (double) ctx.getMetrics().sent);
            }
            // close wavefront proxy connection - required
            closeWavefront();

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * method to get number of available point data since given start epoch second.
     * Note: This is an old method which is no longer being used - subjected for deletion
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public int getNumMetrics(long start, long end) throws Exception {
        Connection conn = null;
        ResultSet rset = null;
        try {
            conn = DBManager.getInstance().getConnection();
            String sql = SQLMySQL.getInstance().genCount(tableName);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, start);    // meaning get all counts
            pstmt.setLong(2, end);      // meaning get all counts
            rset = pstmt.executeQuery();
            if(rset.next()) {
                int count = rset.getInt(1);
                rset.close();
                conn.close();
                rset = null;
                conn = null;
                return count;
            }
        }
        catch(Exception e) {
            throw e;
        }
        finally {
            try {
                if(rset != null) rset.close();
                if(conn != null) conn.close();
            }
            catch(Exception e) {}
        }
        return 0;
    }

    @Override
    public String toString(){
        return this.command;
    }
}
