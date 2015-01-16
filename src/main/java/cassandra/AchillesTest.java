package cassandra;

import cassandra.entities.Ctdbp;
import com.datastax.driver.core.*;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.persistence.*;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

public class AchillesTest {
    Cluster cluster;
    PersistenceManager manager;
    AsyncManager asyncManager;

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .build();
        PersistenceManagerFactory pmf = PersistenceManagerFactory.PersistenceManagerFactoryBuilder
                .builder(cluster)
                .withEntityPackages("cassandra.entities")
                .withKeyspaceName("ooi")
                .forceTableCreation(true).build();

        manager = pmf.createPersistenceManager();
        asyncManager = pmf.createAsyncManager();
    }

    public void close() {
        manager.shutDown();
        cluster.close();
    }

    public List<Ctdbp> createParticles(int count) {
        List<Ctdbp> l = new ArrayList<>();
        for (int i=0; i<count; i++) {
            Ctdbp particle = new Ctdbp();
            Ctdbp.ReferenceDesignator refd = new Ctdbp.ReferenceDesignator("TEST-1-2", System.nanoTime());
            particle.setRefdesig(refd);
            l.add(particle);
        }
        return l;
    }

    public ListenableFuture<Ctdbp> insertParticleAsync(Ctdbp particle, boolean lwt) {
        if (lwt)
            return asyncManager.insert(particle, OptionsBuilder.ifNotExists());
        else
            return asyncManager.insert(particle);
    }

    public void insertParticle(Ctdbp particle, boolean lwt) {
        if (lwt)
            manager.insert(particle, OptionsBuilder.ifNotExists());
        else
            manager.insert(particle);
    }

    public void asyncInsert(List<Ctdbp> particles, boolean lwt, int maxFutures) {
        Stopwatch timer = Stopwatch.createStarted();
        List<ListenableFuture> futures = new ArrayList<>();
        for (Ctdbp particle: particles) {
            futures.add(insertParticleAsync(particle, lwt));
            if (futures.size() > maxFutures) {
                ListenableFuture f = futures.remove(0);
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        for (ListenableFuture f: futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                //e.printStackTrace();
            }
        }

        timer.stop();
        long rate = particles.size() * 1000 / timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("count: " + particles.size() + " elapsed: " + timer + " rate: " + rate);
    }

    public void insert(List<Ctdbp> particles, boolean lwt) {
        Stopwatch timer = Stopwatch.createStarted();
        for (Ctdbp particle: particles) {
            insertParticle(particle, lwt);
        }

        timer.stop();
        long rate = particles.size() * 1000 / timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("count: " + particles.size() + " elapsed: " + timer + " rate: " + rate);
    }

    public void queryAll() {
        Stopwatch timer = Stopwatch.createStarted();
        RegularStatement s = select().from("ctdbp");

        TypedQuery<Ctdbp> tq = manager.rawTypedQuery(Ctdbp.class, s);
        List<Ctdbp> particles = tq.get();
        timer.stop();
        int count = particles.size();
        long rate = count * 1000 / timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("count: " + count + " elapsed: " + timer + " rate: " + rate);
    }

    public void queryAll2() {
        Stopwatch timer = Stopwatch.createStarted();
        RegularStatement s = select().from("ctdbp");

        TypedQuery<Ctdbp> tq = manager.typedQuery(Ctdbp.class, s);
        List<Ctdbp> particles = tq.get();
        timer.stop();
        int count = particles.size();
        long rate = count * 1000 / timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("count: " + count + " elapsed: " + timer + " rate: " + rate);
    }

    public void queryAll3() {
        Stopwatch timer = Stopwatch.createStarted();
        List<Ctdbp> particles = manager.sliceQuery(Ctdbp.class)
                .forSelect()
                .withPartitionComponents("TEST", "1", "2")
                .fromClusterings(1421356340000000000l)
                .toClusterings(System.nanoTime())
                .get();
        for (Ctdbp particle : particles) {
            particle.getRefdesig();
        }
        int count = particles.size();
        long rate = count * 1000 / timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("count: " + count + " elapsed: " + timer + " rate: " + rate);
    }
}
