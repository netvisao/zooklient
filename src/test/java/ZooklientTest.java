import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by aminerounak on 3/4/14.
 */
public class ZooklientTest {


    private String connStr;
    private ZooPropKlient client;


    @Before public void setup() throws IOException {


        connStr = System.getenv("zkCon");
        if (connStr == null) {
            connStr =
                    "zconf-a01.white.aol.com:2181," +
                    "zconf-a02.white.aol.com:2181," +
                    "zconf-a03.white.aol.com:2181";
        }


        client = new ZooPropKlient(connStr, "/auth");
        client.connect();
    }

    @After public void tearDown() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {

            }
        }
    }

    @Test public void testZooPropKlientData() throws IOException {
        String k0 = "name", k1 = "name.key" ;
        String rnd0 = "" + System.currentTimeMillis();
        String rnd1 = "" + System.currentTimeMillis() + 1;

        assertTrue(client.putDataStr(k0, rnd0));
        String res0 = client.getDataStr(k0);
        assertEquals(rnd0, res0);

        assertTrue(client.putDataStr(k1, rnd1));
        res0 = client.getDataStr(k1);
        assertEquals(rnd1, res0);
    }

}
