package com.vmware.wavefront.integration.zabbix;

/**
 * class supplying SQL queries for MySQL database, nothing more
 */
public class SQLMySQL {

    public static SQLMySQL mysql;
    static {
        mysql = new SQLMySQL();
    }

    public static SQLMySQL getInstance() {
        return mysql;
    }

    protected String genBaseQuery(String table) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(" FROM ");
        buffer.append(table);
        buffer.append(" AS hi ");
        buffer.append("INNER JOIN items AS i on hi.itemid = i.itemid ");
        buffer.append("INNER JOIN hosts AS h use index(hosts_1) ON i.hostid = h.hostid ");
        buffer.append(" WHERE hi.itemid IN (SELECT DISTINCT itemid FROM items)");
        return buffer.toString();
    }

    public String genQuery(String table) {

        StringBuffer buffer = new StringBuffer();
        buffer.append("SELECT hi.clock, hi.value, h.host, i.key_");
        buffer.append(genBaseQuery(table));
        buffer.append(" AND hi.clock > ?");
        return buffer.toString();
    }

    public String genCount(String table) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SELECT count(*) count");
        buffer.append(genBaseQuery(table));
        buffer.append(" AND hi.clock > ? AND hi.clock <= ?");
        return buffer.toString();
    }

    public String genQueryLimit(String table) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(genQuery(table));
        buffer.append(" LIMIT ?");
        return buffer.toString();
    }
}
