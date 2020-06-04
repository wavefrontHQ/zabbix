package com.vmware.wavefront.integration.zabbix;

import java.util.*;

/**
 * map that stores contexts
 */
public class ContextMap extends HashMap<String, Context> {

    public ContextMap() {
        super();
    }

    public void addContext(Context ctx) {
        put(ctx.getTableName(), ctx);
    }

    public Context getContext(String name) {
        return get(name);
    }

    public List getAllContexts() {
        return new ArrayList(this.values());
    }
}
