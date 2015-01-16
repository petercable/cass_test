package cassandra;

import cassandra.entities.Ctdbp;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
//        SimpleClient client = new SimpleClient();
//        client.connect("localhost");
//        client.querySync();
//        client.queryAsync();
//        client.queryMapped();
//        client.queryMappedAsync();
//        client.queryMappedByDay("TEST", 2014, 1);
//        client.queryAllDaysMapped();
//        client.queryAllDays();
//
//        client.storeParticlesSync(client.createParticles(100000));
//        client.storeParticleAsync(client.createParticles(100000), 1000);
//        client.close();
        AchillesTest at = new AchillesTest();
        at.connect("localhost");
        // Dupe test
        List<Ctdbp> particles = at.createParticles(1000);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.insert(particles, false);
        at.asyncInsert(particles, false, 1000);
        at.asyncInsert(particles, true, 1000);
//        at.insert(10000, true);
//        at.asyncInsert(10000, true, 1000);
//        at.queryAll();
//        at.queryAll2();
//        at.queryAll3();
        at.close();
    }
}
