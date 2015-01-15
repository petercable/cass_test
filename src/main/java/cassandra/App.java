package cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        SimpleClient client = new SimpleClient();
        client.connect("localhost");
//        client.querySync();
//        client.queryAsync();
//        client.queryMapped();
//        client.queryMappedAsync();
//        client.queryMappedByDay("TEST", 2014, 1);
//        client.queryAllDaysMapped();
//        client.queryAllDays();

        client.createParticles(10000);
        client.close();
    }
}
