package cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SimpleClient {
    private Cluster cluster;
    private Session session;
    MappingManager manager;
    CtdbpCdefCpInstrumentRecoveredAccessor accessor;
    Stopwatch timer = Stopwatch.createUnstarted();

    private Logger log = Logger.getLogger("SimpleClient");
    private String selectAll = "select * from ctdbp_cdef_cp_instrument_recovered";

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .build();

        Metadata metadata = cluster.getMetadata();
        session = cluster.connect("ooi");
        manager = new MappingManager(session);
        accessor = manager.createAccessor(CtdbpCdefCpInstrumentRecoveredAccessor.class);
    }

    public void close() {
        cluster.close();
    }

    /*
     * Exists to at least access all members of a row
     * To compare performance with object mapper...
     */
    private void scanRow(Row row) {
        row.getString("refdesig");
        row.getInt("year");
        row.getInt("jday");
        row.getLong("time");
        row.getInt("conductivity");
        row.getInt("ctd_time");
        row.getDouble("driver_timestamp");
        row.getDouble("ingestion_timestamp");
        row.getDouble("internal_timestamp");
        row.getString("preferred_timestamp");
        row.getInt("pressure");
        row.getInt("pressure_temp");
        row.getInt("temperature");
    }

    private void logElapsed(long count) {
        timer.stop();
        long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
        long rate = 0;
        if (elapsed > 0)
            rate = count * 1000 / elapsed;

        log.info("Done iterating resultset, count: " + count + " elapsed: " + timer + " rate: " + rate + "/s");
        timer.reset();
    }

    public void querySync() {
        log.info("query sync");
        long count = 0;
        timer.start();
        ResultSet rs = session.execute(selectAll);
        for (Row row: rs) {
            scanRow(row);
            count++;
        }
        logElapsed(count);
    }

    public void queryAsync() {
        log.info("query async");
        long count = 0;
        timer.start();
        ResultSetFuture rs = session.executeAsync(selectAll);
        for (Row row: rs.getUninterruptibly()) {
            scanRow(row);
            count++;
        }
        logElapsed(count);
    }

    public void queryMapped() {
        log.info("query mapped");
        long count = 0;
        timer.start();
        Result<CtdbpCdefCpInstrumentRecovered> results = accessor.getAll();
        for (CtdbpCdefCpInstrumentRecovered r: results) {
            r.getRefdesig();
            count++;
        }
        logElapsed(count);
    }

    public void queryMappedAsync() throws ExecutionException, InterruptedException {
        log.info("query mapped async");
        long count = 0;
        timer.start();
        List<CtdbpCdefCpInstrumentRecovered> particles = new ArrayList<>();
        ListenableFuture<Result<CtdbpCdefCpInstrumentRecovered>> results = accessor.getAllAsync();
        for (CtdbpCdefCpInstrumentRecovered r: results.get()) {
            particles.add(r);
            count++;
        }
        int size = particles.size();
        logElapsed(count);
    }

    public void queryMappedByDay(String refdesig, int year, int day) {
        log.info("query mappedByDay");
        long count = 0;
        timer.start();
        Result<CtdbpCdefCpInstrumentRecovered> results = accessor.getDay(refdesig, year, day);
        for (CtdbpCdefCpInstrumentRecovered r: results) {
            r.getRefdesig();
            count++;
        }
        logElapsed(count);
    }

    public void queryAllDaysMapped() throws ExecutionException, InterruptedException {
        log.info("query all days async (mapped)");
        long count = 0;
        timer.start();
        ResultSet rs = accessor.getDistinct();
        List<ListenableFuture<Result<CtdbpCdefCpInstrumentRecovered>>> futureList = new ArrayList<>();
        for (Row row: rs) {
            futureList.add(accessor.getDayAsync(row.getString(0), row.getInt(1), row.getInt(2)));
        }
        for (ListenableFuture<Result<CtdbpCdefCpInstrumentRecovered>> result: futureList) {
            for (CtdbpCdefCpInstrumentRecovered r: result.get()) {
                r.getRefdesig();
                count++;
            }
        }
        logElapsed(count);
    }

    public void queryAllDays() throws ExecutionException, InterruptedException {
        log.info("query all days async (unmapped)");
        long count = 0;

        Select distinct = QueryBuilder
                .select()
                .distinct()
                .column("refdesig").column("year").column("jday")
                .from("ctdbp_cdef_cp_instrument_recovered");

        timer.start();
        List<ResultSetFuture> futures = new ArrayList<>();

        for (Row row: session.execute(distinct)) {
            Select.Where oneDay = QueryBuilder
                    .select()
                    .all()
                    .from("ctdbp_cdef_cp_instrument_recovered")
                    .where(QueryBuilder.eq("refdesig", row.getString(0)))
                    .and(QueryBuilder.eq("year", row.getInt(1)))
                    .and(QueryBuilder.eq("jday", row.getInt(2)));

            futures.add(session.executeAsync(oneDay));
        }
        for (ResultSetFuture result: futures) {
            for (Row row: result.getUninterruptibly()) {
                scanRow(row);
                count++;
            }
        }
        logElapsed(count);
    }

    public List<CtdbpCdefCpInstrumentRecovered> createParticles(long count) {
        log.info("create particles...");
        timer.start();

        List<CtdbpCdefCpInstrumentRecovered> particles = new ArrayList<>();
        for (long i=0; i<count; i++) {
            CtdbpCdefCpInstrumentRecovered particle = new CtdbpCdefCpInstrumentRecovered();
            particle.fill();
            particles.add(particle);
        }
        logElapsed(count);
        return particles;
    }

    public void storeParticlesSync(List<CtdbpCdefCpInstrumentRecovered> particles) {
        log.info("store sync");
        timer.start();
        Mapper<CtdbpCdefCpInstrumentRecovered> mapper = new MappingManager(session).mapper(CtdbpCdefCpInstrumentRecovered.class);
        for (CtdbpCdefCpInstrumentRecovered particle: particles) {
            mapper.save(particle);
        }
        logElapsed(particles.size());
    }

    public void storeParticleAsync(List<CtdbpCdefCpInstrumentRecovered> particles, int maxFutures) {
        log.info("store async");
        timer.start();
        List<ListenableFuture> futures = new ArrayList<>();
        Mapper<CtdbpCdefCpInstrumentRecovered> mapper = new MappingManager(session).mapper(CtdbpCdefCpInstrumentRecovered.class);
        for (CtdbpCdefCpInstrumentRecovered particle: particles) {
            futures.add(mapper.saveAsync(particle));
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
                e.printStackTrace();
            }
        }
        logElapsed(particles.size());
    }
}
