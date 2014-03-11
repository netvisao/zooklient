import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by aminerounak on 3/4/14.
 */
public class ZooPropKlient implements Closeable, Configurable {

    public static final Stat STATS = new Stat();
    public static final String CHARSET__UTF_8 = "utf-8";
    public static final String EMPTY = "";
    public static final String PROP_SEP = ".";
    private ZooKeeper zk;
    private final String connStr;
    private final String zkTopLevel;
    private int connectTimeout = 10000;
    private final ConcurrentMap<String, String> data = new ConcurrentHashMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(ZooPropKlient.class);
    private static final String Bool0 = "0";
    private static final String Bool1 = "1";
    private static final String ZK_NODE_SEP = "/";

    private class Watch implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {

            if (watchedEvent.getPath().startsWith(zkTopLevel)) {
                if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {

                    String prop = nodename2Prop(watchedEvent.getPath());
                    data.put(prop, getData(prop));

                } else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {

                    String prop = nodename2Prop(watchedEvent.getPath());
                    data.put(prop, null);

                }
            }
        }
    }

    public ZooPropKlient(String connString, String root) {
        this.connStr = connString;
        this.zkTopLevel = root;
    }

    public ZooPropKlient(String connString, String root, int connectTimeout) {
        this(connString, root);
        this.connectTimeout = connectTimeout;
    }

    public void connect() throws IOException {
        zk = new ZooKeeper(connStr, connectTimeout, new Watch());
    }


    private boolean putData(String prop, String val) {

        String node = prop2Nodename(prop);

        try {
            if (val == null) {

                zk.delete(node, -1);
                data.put(prop, null);
                return true;
            } else {

                byte[] bytes = val.getBytes(CHARSET__UTF_8);
                zk.setData(node, bytes, -1);
                data.put(prop, val);
                return true;
            }

        } catch (InterruptedException e) {
            LOG.error("INTERRUPTED_EX_PUT {} {}", prop, e.getMessage());
            return false;
        } catch (KeeperException e) {
            LOG.error("KEEPER_EX_PUT {} {}", prop, e.getMessage());
            return false;
        } catch (UnsupportedEncodingException e) {
            LOG.error("CHARSET_EX_PUT {} {}", prop, e.getMessage());
            return false;
        }
    }

    private String getData(String prop) {

        String res = null;
        try {

            if (!data.containsKey(prop)) {
                byte[] bytes = zk.getData(prop2Nodename(prop), true, STATS);
                data.put(prop, new String(bytes, CHARSET__UTF_8));
                return data.get(prop);
            }

            res = data.get(prop);


        } catch (KeeperException.NoNodeException e) {
            res = data.put(prop, null);

        } catch (KeeperException e) {
            LOG.error("KEEPER_EX {} {}", prop, e.getMessage());

        } catch (InterruptedException e) {
            LOG.error("INTERRUPTED_EX {} {}", prop, e.getMessage());

        } catch (UnsupportedEncodingException e) {
            LOG.error("CHARSET_EX {} {}", prop, e.getMessage());
        }


        return res;
    }

    @Override
    public void close() throws IOException {
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.error("INTERRUPTED_EX_ONCLOSE {}", e.getMessage());
        }
    }


    private String prop2Nodename(String prop) {
        return zkTopLevel + ZK_NODE_SEP + prop.replace(PROP_SEP, ZK_NODE_SEP);
    }

    private String nodename2Prop(String name) {

        if (name.startsWith(zkTopLevel))
            name = name.length() == zkTopLevel.length()
                    ? EMPTY
                    : name.substring(zkTopLevel.length());

        return name.replace(ZK_NODE_SEP, PROP_SEP);
    }

    public Long getDataLong(String k) {

        String s = getDataStr(k);

        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Integer getDataInt(String k) {

        String s = getDataStr(k);

        try {
            return Integer.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Boolean getDataBool(String k) {

        String s = getDataStr(k);
        if (s == null)
            return null;

        return !(s.isEmpty() || s.replace(Bool0, EMPTY).isEmpty());
    }

    public String getDataStr(String k) {
        return getData(k);
    }

    public boolean putDataStr(String k, String v) {
        return putData(k, v);
    }

    public boolean putDataBool(String k, Boolean v) {
        if (v == null)
          return putData(k, null);
        else
          return putData(k, v ? Bool1 : Bool0);
    }

    public boolean putDataLong(String k, Long v) {
        if (v == null)
            return putData(k, null);
        else
            return putData(k, EMPTY + v);

    }

    public boolean putDataInt(String k, Integer v) {
        if (v == null)
            return putData(k, null);
        else
            return putData(k, EMPTY + v);
    }

}
