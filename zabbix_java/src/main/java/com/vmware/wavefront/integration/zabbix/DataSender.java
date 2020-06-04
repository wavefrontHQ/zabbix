package com.vmware.wavefront.integration.zabbix;

import com.wavefront.integrations.Wavefront;

import java.sql.*;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * data sender responsible for querying zabbix database, extracting point data,
 * and sending it to wavefront proxy.
 */
public class DataSender extends DataRunner implements Runnable {

    private static Pattern punc_whitespace;
    private static Pattern trailing_dots;
    private static Pattern one_dot;
    static Logger logger = LoggerFactory.getLogger(com.vmware.wavefront.integration.zabbix.DataSender.class);

    static {
        // regex patterns to reformat key to wavefront compatible metrics name
        punc_whitespace = Pattern.compile("[^\\w\\s\\-.]|\\s+");
        trailing_dots = Pattern.compile("\\.+$");
        one_dot = Pattern.compile("\\.+");
    }

    DataFetcher fetcher;
    String table;
    int startLimit;
    int endLimit;
    long startTime;
    long endTime;

    public DataSender(DataFetcher fetcher, long startTime, long endTime, int startLimit, int endLimit){
        super(fetcher.getContext());
        this.fetcher = fetcher;
        this.table = fetcher.getTable();
        this.startLimit = startLimit;
        this.endLimit = endLimit;
        this.startTime = startTime;
        this.endTime = endTime;
        command = "[DataSender] " + table + " time:" + startTime + "~" + endTime + " range:" + startLimit + " ~ " + endLimit;
    }

    public void run() {
        logger.debug(Thread.currentThread().getName()+" Start. Command = "+command);
        processCommand();
        logger.debug(Thread.currentThread().getName()+" End.");
    }

    private void processCommand() {
        Connection conn = null;
        ResultSet rset = null;
        Wavefront wavefront = null;
        long latesttime = 0;
        try {
            String sql = SQLMySQL.getInstance().genQueryLimit(table);
            logger.debug("[sql]" + sql + " [" + startTime + "," + endLimit + "]");

            // run query to extract the data
            long startConn = System.currentTimeMillis();
            conn = DBManager.getInstance().getConnection();
            long endConn = System.currentTimeMillis();
            endConn = endConn - startConn;
            ctx.getMetrics().addDbConnTime(endConn);

            long startPstmt = System.currentTimeMillis();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            long endPstmt = System.currentTimeMillis();
            endPstmt = endPstmt - startPstmt;
            ctx.getMetrics().addDbQueryPrepTime(endPstmt);

            pstmt.setLong(1, startTime);
            // pstmt.setLong(2, endTime);
            // pstmt.setInt(3, startLimit);
            pstmt.setInt(2, endLimit);

            String wfhost = ZabbixConfig.getProperty("wavefront.proxy.host");
            String wfport = ZabbixConfig.getProperty("wavefront.proxy.port");
            String prefix = ZabbixConfig.getProperty("global.prefix");
            if(prefix != null) prefix = prefix.trim();
            else prefix = "";
            // append prefix if prefix exists
            if(prefix.length() > 0) prefix = prefix + ".";

            // get wavefront connection going
            if(ZabbixConfig.exists("integration.wavefront.send") && ZabbixConfig.getProperty("integration.wavefront.send").equalsIgnoreCase("true")) {
                wavefront = new Wavefront(wfhost, Integer.parseInt(wfport));
            }

            long start_query = System.currentTimeMillis();
            rset = pstmt.executeQuery();

            // simulated delays
            /*
            long delay = randomWithRange(1,20) * 1000l;
            logger.debug(Thread.currentThread().getName() + " delay " + delay + "ms..");
            Thread.currentThread().sleep(delay);
            */

            long end_query = System.currentTimeMillis();
            end_query = end_query - start_query;
            ctx.getMetrics().addDbQueryExecTime(end_query);

            long sendTotalMs = 0;
            int sent = 0;

            long start_itr = System.currentTimeMillis();
            while(rset.next()) {
                // convert each rows, and submit to WF proxy
                long startSend = System.currentTimeMillis();
                long clock = rset.getLong("clock");
                String value = rset.getString("value");
                String host = reformatSource(rset.getString("host"));
                String key = reformatMetricName(rset.getString("key_"));

                String out = String.format("%s%s %s %d source=%s", prefix, key, value, clock, host);

                // send the out to socket.
                if(wavefront != null) {
                    if(prefix != null) key = prefix + key;
                    wavefront.send(key, Double.parseDouble(value), clock, host);
                } else {
                    logger.debug(out);
                }
                sent++;
                long endSend = System.currentTimeMillis();
                endSend = endSend - startSend;
                sendTotalMs += endSend;
            }
            long end_itr = System.currentTimeMillis();
            end_itr = end_itr - start_itr;
            ctx.getMetrics().addDbResultItrTime(end_itr);
            ctx.getMetrics().addWfSendTime(sendTotalMs);
            ctx.getMetrics().addSent(sent);

            if(wavefront != null) {
                wavefront.flush();
                wavefront.close();
            }
        } catch(SQLException sqle) {
            sqle.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(rset != null) try { rset.close(); } catch(Exception e) {}
            if(conn != null) try { conn.close(); } catch(Exception e) {}
            if(wavefront != null) try {wavefront.close(); } catch(Exception e) {}
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
