package xx.yy;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.result.ResultIterator;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.FetchSize;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbiConcurrencyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbiConcurrencyTest.class);

    @Rule
    public JdbiRule postgres = JdbiRule.embeddedPostgres();

    @Test
    public void isAlive() {
        int one = postgres.getJdbi().withHandle(h -> h.createQuery("select 1").mapTo(Integer.class).one());
        Assert.assertEquals(1, one);
    }

    /**
     * Proof that the SQL works
     */
    @Test
    public void simpleReadTest() {
        postgres.getJdbi().installPlugin(new SqlObjectPlugin());
        final Handle handle = postgres.getJdbi().open();
        try (ResultIterator<TSVRecord> series = readTsForTestingDAO1(handle, 1, 0, 1_000_000)) {
            int count = 0;
            while (series.hasNext()) {
                final TSVRecord record = series.next();
                count++;
            }
            System.out.println(count);
        }
        try (ResultIterator<TSVRecord> series = readTsForTestingDAO2(handle, 1, 0, 1_000_000)) {
            int count = 0;
            while (series.hasNext()) {
                final TSVRecord record = series.next();
                count++;
            }
            System.out.println(count);
        }
        handle.close();
    }

    /**
     * Reproduce https://github.com/jdbi/jdbi/issues/2754
     */
    @Test
    public void parallelReadTest() {
        postgres.getJdbi().installPlugin(new SqlObjectPlugin());
        try (final Handle handle = postgres.getJdbi().open()) {
            final List<SourceStreamEntry> results = readSourceStreamEntriesParallel(
                    handle,
                    IntStream.range(0, 100).boxed().toList(), // if this number is too small, the error won't show up in this test case
                    0,
                    1_000_000
            );
            LOGGER.debug("Starting reads ...");
            readLateral(results);
        }
    }

    private static void readLateral(List<SourceStreamEntry> entries) {
        final List<Iterator<TSVRecord>> iters = entries.stream().map(SourceStreamEntry::stream1).collect(Collectors.toList());
        int count = 0;
        while (iters.get(0).hasNext()) {
            final List<TSVRecord> next = iters.stream().map(Iterator::next).toList();
            count++;
        }
        System.out.println(count);
    }

    record SourceStreamEntry(
            long sensorId,
            ResultIterator<TSVRecord> stream1,
            ResultIterator<TSVRecord> stream2
    ) {
    }

    /**
     * Parallel, multi-threaded version, causing issues
     */
    List<SourceStreamEntry> readSourceStreamEntriesParallel(Handle handle, List<Integer> sourceSegments, long tMin, long tMax) {
        LOGGER.debug("Performing parallel queries: {}", sourceSegments.size());
        final long startNanos = System.nanoTime();
        try (
                final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()
        ) {
            List<Callable<SourceStreamEntry>> tasks = new ArrayList<>();
            int pos = 0;
            for (final long segment : sourceSegments) {
                LOGGER.debug("Performing query: {}", pos);
                final Callable<SourceStreamEntry> task = () -> {
                    final ResultIterator<TSVRecord> stream1 = readTsForTestingDAO1(handle, segment, tMin, tMax); // this makes a singe SQL query internally (read-only) via JDBI Handle
                    // this second stream seems to be crucial to produce the error. Without it, the test runs fine
                    final ResultIterator<TSVRecord> stream2 = readTsForTestingDAO2(handle, segment, tMin, tMax);
                    return new SourceStreamEntry(segment, stream1, stream2);
                };
                tasks.add(task);
                pos++;
            }
            final List<Future<SourceStreamEntry>> results = executor.invokeAll(tasks);

            final List<SourceStreamEntry> retval = new ArrayList<>();
            for (Future<SourceStreamEntry> result : results) {
                assert result.isDone();
                if (result.isCancelled()) {
                    throw new RuntimeException("At least one DB request was cancelled.");
                }
                try {
                    final SourceStreamEntry entry = result.get();
                    retval.add(entry);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            final long endNanos = System.nanoTime();
            final Duration queryTime = Duration.ofNanos(endNanos - startNanos);
            LOGGER.debug("Query time: {}", queryTime);
            return retval;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sequential, single-threaded version of {@link #readSourceStreamEntriesParallel}, working fine
     */
    List<SourceStreamEntry> readSourceStreamEntriesSequential(Handle handle, List<Integer> sourceSegments, long tMin, long tMax) {
        LOGGER.debug("Performing sequential queries: {}", sourceSegments.size());
        final long startNanos = System.nanoTime();
        final List<SourceStreamEntry> retval = new ArrayList<>();
        for (final long segment : sourceSegments) {
            final ResultIterator<TSVRecord> stream1 = readTsForTestingDAO1(handle, segment, tMin, tMax);
            final ResultIterator<TSVRecord> stream2 = readTsForTestingDAO2(handle, segment, tMin, tMax);
            final SourceStreamEntry entry = new SourceStreamEntry(segment, stream1, stream2);
            retval.add(entry);
        }

        final long endNanos = System.nanoTime();
        final Duration queryTime = Duration.ofNanos(endNanos - startNanos);
        LOGGER.debug("Query time: {}", queryTime);
        return retval;
    }

    ResultIterator<TSVRecord> readTsForTestingDAO1(Handle handle, long seriesId, long tStart, long tEnd) {
        pause(100); // simulate initial DB latency
        return handle.attach(DAO1.class).readTimeSeries(seriesId, tStart, tEnd);
    }

    ResultIterator<TSVRecord> readTsForTestingDAO2(Handle handle, long seriesId, long tStart, long tEnd) {
        pause(100); // simulate initial DB latency
        return handle.attach(DAO2.class).readTimeSeries(seriesId, tStart, tEnd);
    }

    private void pause(int sleepMillis) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    interface DAO1 {
        @SqlQuery("SELECT :seriesId AS series_id, timestamp, random()*100 AS value FROM generate_series(:tStart,:tEnd,1000) AS timestamp;")
        @RegisterRowMapper(TSVMapper.class)
        @FetchSize(6)
        ResultIterator<TSVRecord> readTimeSeries(
                @Bind("seriesId") long seriesId,
                @Bind("tStart") final long tStart,
                @Bind("tEnd") final long tEnd
        );

        final class TSVMapper implements RowMapper<TSVRecord> {

            @Override
            public TSVRecord map(ResultSet rs, StatementContext ctx) throws SQLException {
                return new TSVRecord(
                        rs.getLong("series_id"),
                        rs.getLong("timestamp"),
                        rs.getDouble("value")
                );
            }
        }
    }

    interface DAO2 {
        @SqlQuery("SELECT :seriesId AS series_id, timestamp, random()*100 AS value FROM generate_series(:tStart,:tEnd,1000) AS timestamp;")
        @RegisterRowMapper(DAO1.TSVMapper.class)
        @FetchSize(6)
        ResultIterator<TSVRecord> readTimeSeries(
                @Bind("seriesId") long seriesId,
                @Bind("tStart") final long tStart,
                @Bind("tEnd") final long tEnd
        );
    }

    record TSVRecord(
            long seriesId,
            long epochMillis,
            double value
    ) {
    }

}
