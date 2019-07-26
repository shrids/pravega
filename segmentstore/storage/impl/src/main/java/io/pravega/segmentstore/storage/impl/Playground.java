package io.pravega.segmentstore.storage.impl;

import io.pravega.common.Timer;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryCache;
import java.util.Random;
import lombok.Cleanup;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.slf4j.LoggerFactory;

@Slf4j
public class Playground {

    public static void main(String[] args) throws Exception {
//        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
//        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();

        final int bufSize = 10 * 1024;
        final int keyCount = 1_000_000;
        final int iterationCount = 5;
        for (int i = 0; i < iterationCount; i++) {
            //val r = testInMemoryCache(bufSize, keyCount);
            val r = testRocksDbCache(bufSize, keyCount);

            System.out.println(String.format("Insert: %d, Get: %d, Remove: %d", r.insertMillis, r.getMillis, r.removeMillis));
        }
    }

    private static Result testInMemoryCache(int bufSize, int count) {
        @Cleanup
        val c = new InMemoryCache("Testing");
        return testClassicCache(c, bufSize, count);
    }

    private static Result testRocksDbCache(int bufSize, int count) {
        @Cleanup
        val f = new RocksDBCacheFactory(RocksDBConfig.builder()
                                                     .with(RocksDBConfig.READ_CACHE_SIZE_MB, 8)
                                                     .with(RocksDBConfig.WRITE_BUFFER_SIZE_MB, 64)
                                                     .build());
        @Cleanup
        val c = f.getCache("Test");
        return testClassicCache(c, bufSize, count);
    }

    private static Result testClassicCache(Cache c, int bufSize, int count) {
        val buffer = new ByteArraySegment(new byte[bufSize]);
        new Random(0).nextBytes(buffer.array());
        System.gc();
        val insertTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.insert(new CacheKey(i), buffer);
        }
        long insertMillis = insertTimer.getElapsedMillis();

        System.gc();
        val getTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.get(new CacheKey(i));
        }
        long getMillis = getTimer.getElapsedMillis();

        System.gc();
        val removeTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.remove(new CacheKey(i));
        }
        long removeMillis = removeTimer.getElapsedMillis();

        return new Result(insertMillis, getMillis, removeMillis);
    }

    @RequiredArgsConstructor
    private static class CacheKey extends Cache.Key {
        private final int id;

        @Override
        public byte[] serialize() {
            byte[] r = new byte[4];
            BitConverter.writeInt(r, 0, this.id);
            return r;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CacheKey) {
                return this.id == ((CacheKey) obj).id;
            }

            return false;
        }
    }

    @Data
    private static class Result {
        final long insertMillis;
        final long getMillis;
        final long removeMillis;
    }
}