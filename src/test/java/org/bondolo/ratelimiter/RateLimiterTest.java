/*
 * Copyright © 2020 Mike Duigou
 */
package org.bondolo.ratelimiter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RateLimiter
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class RateLimiterTest {

    /**
     * Test of setPermitRate method, of class RateLimiter.
     */
    @Test
    public void testSetPermitRate() {
        System.out.println("setPermitRate");
        int permits = 42;
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofNanos(1));
        instance.setPermitRate(permits);
        int result = instance.getPermitRate();
        assertEquals(permits, result);
    }

    /**
     * Test of setPermitInterval method, of class RateLimiter.
     */
    @Test
    public void testSetPermitInterval() {
        System.out.println("setPermitRate");
        Duration interval = Duration.ofNanos(42);
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofNanos(1));
        instance.setPermitInterval(interval);
        Duration result = instance.getPermitInterval();
        assertEquals(interval, result);
    }

    /**
     * Test of acquire method, of class RateLimiter.
     * @throws java.lang.Exception for failures
     */
    @Test
    public void testAcquire() throws Exception {
        System.out.println("acquire");
        String bucket = "test";
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofNanos(1));
        instance.acquire(bucket);
        assertEquals(1, instance.claimed(bucket));
    }

    /**
     * Test of acquire method, of class RateLimiter.
     * @throws java.lang.Exception for failures
     */
    @Test
    public void testAcquire_int() throws Exception {
        System.out.println("acquire");
        String bucket = "test";
        int permits = 200;
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofNanos(1));
        instance.acquire(bucket, 1);
        long start = System.nanoTime();
        instance.acquire(bucket, permits);
        long end = System.nanoTime();
        assertThat(permits).isLessThanOrEqualTo(((int) TimeUnit.NANOSECONDS.toMillis(end - start)));
    }

    /**
     * Test of tryAcquire method, of class RateLimiter.
     * @throws java.lang.Exception for failures
     */
    @Test
    public void testTryAcquire() throws Exception {
        System.out.println("tryAcquire");
        String bucket = "test";
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofDays(1));
        assertTrue(instance.tryAcquire(bucket));
        assertFalse(instance.tryAcquire(bucket));
        instance.setPermitInterval(Duration.ofMillis(1));
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(1));
        assertTrue(instance.tryAcquire(bucket));
    }

    /**
     * Test of tryAcquire method, of class RateLimiter.
     * @throws java.lang.Exception for failures
     */
    @Test
    public void testTryAcquire_int() throws Exception {
        System.out.println("tryAcquire");
        String bucket = "test";
        int permits = 2;
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofDays(1));
        assertTrue(instance.tryAcquire(bucket));
        assertFalse(instance.tryAcquire(bucket));
        instance.setPermitInterval(Duration.ofMillis(1));
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(permits));
        assertTrue(instance.tryAcquire(bucket));
    }

    /**
     * Test of tryAcquire method, of class RateLimiter.
     */
    @Test
    public void testTryAcquireMulti() {
        System.out.println("tryAcquireMulti");
        String one = "one";
        String two = "two";
        String three = "three";
        AtomicLong clock = new AtomicLong(0);
        RateLimiter<String> instance = new RateLimiter<>(1, 1, 2, 2, 2, Duration.ofNanos(1), System::nanoTime);
        assertTrue(instance.tryAcquire(one));
        assertFalse(instance.tryAcquire(one));
        assertTrue(instance.tryAcquire(two));
        assertEquals(1, instance.claimed(two));
        assertEquals(0, instance.denied(two));
        assertFalse(instance.tryAcquire(two));
        assertEquals(1, instance.denied(two));
        assertTrue(instance.tryAcquire(three));
        clock.incrementAndGet(); // tick
        assertTrue(instance.tryAcquire(one));
        assertFalse(instance.tryAcquire(one));
        assertTrue(instance.tryAcquire(two));
        assertFalse(instance.tryAcquire(two));
        assertFalse(instance.tryAcquire(three)); // none in source
        clock.incrementAndGet(); // tick
        clock.incrementAndGet(); // tick
        assertTrue(instance.tryAcquire(one));
        assertFalse(instance.tryAcquire(one));
        assertTrue(instance.tryAcquire(two));
        assertFalse(instance.tryAcquire(three)); // none in source
    }

    @Test
    public void testBuckets() {
        System.out.println("buckets");
        Set<String> buckets = new HashSet<>(Arrays.asList( null, "one", "two", "three"));
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofDays(1));
        for (String bucket : buckets) {
            assertTrue(instance.tryAcquire(bucket));
        }
        Set<String> known = instance.buckets().collect(Collectors.toSet());
        assertEquals(buckets, known);
    }

    @Test
    public void testAvailable() {
        System.out.println("available");
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofDays(1));
        assertTrue(0 != instance.available("available"));
    }

    @Test
    public void testRemove() {
        System.out.println("remove");
        RateLimiter<String> instance = new RateLimiter<>(1, Duration.ofDays(1));
        assertTrue(0 != instance.available("remove"));
        assertTrue(instance.remove("remove"));
        assertFalse(instance.remove("remove"));
    }

    private static final int TEST_DURATION_SECONDS = 5;
    private static final int TEST_RATE = 100;

    /**
     *
     * @throws java.lang.Exception for failures
     */
    @Test
    public void testMultiThreads() throws Exception {
        System.out.println("testMultiThreads");
        int cores = ForkJoinPool.getCommonPoolParallelism() - 1;
        System.out.println("cores:" + cores);
        RateLimiter<String> instance = new RateLimiter<>(1, TEST_RATE, TEST_RATE, cores * TEST_RATE, cores * TEST_RATE, Duration.ofSeconds(1), System::nanoTime);

        System.out.println(instance);
        List<Callable<Integer>> tasks = IntStream.range(0, cores)
                .mapToObj(worker -> makeWorker(instance, worker, TEST_DURATION_SECONDS))
                .collect(Collectors.toList());

        List<Future<Integer>> futures = ForkJoinPool.commonPool().invokeAll(tasks);

        System.out.println(instance);

        futures.forEach(future -> {
            try {
                int acquired = future.get(TEST_DURATION_SECONDS, TimeUnit.SECONDS);
                assertThat(acquired)
                        .isGreaterThanOrEqualTo(TEST_RATE * (TEST_DURATION_SECONDS - 1))
                        .isLessThanOrEqualTo(TEST_RATE * (TEST_DURATION_SECONDS + 1));
                System.out.println(acquired);
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                ex.printStackTrace(System.err);
                fail(ex.toString());
            }
        });

        System.out.println(instance);

        // a different set of buckets

        tasks = IntStream.range(cores, cores * 2)
                .mapToObj(worker -> makeWorker(instance, worker, TEST_DURATION_SECONDS))
                .collect(Collectors.toList());

        futures = ForkJoinPool.commonPool().invokeAll(tasks);

        futures.forEach(future -> {
            try {
                int acquired = future.get(TEST_DURATION_SECONDS, TimeUnit.SECONDS);
                assertThat(acquired)
                        .isGreaterThanOrEqualTo(TEST_RATE * (TEST_DURATION_SECONDS - 1))
                        .isLessThanOrEqualTo(TEST_RATE * (TEST_DURATION_SECONDS + 1));
                System.out.println(acquired);
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                ex.printStackTrace(System.err);
                fail(ex.toString());
            }
        });

        // Testing that buckets expire
        for (int x = 1; x < TEST_RATE * cores; x++) {
            instance.acquire("another");
        }

        System.out.println(instance);
    }

    private Callable<Integer> makeWorker(RateLimiter<String> governor, int id, int seconds) {
        String name = "task #" + id;
        return () -> {
            int acquired = 0;
            long remainingNanos = TimeUnit.SECONDS.toNanos(seconds);
            long deadlineNanos = System.nanoTime() + remainingNanos;

            while ((remainingNanos = deadlineNanos - System.nanoTime()) > 0) {
                while (governor.tryAcquire(name)) {
                    acquired++;
                }

                governor.acquire(name);
                acquired++;
            }

            return acquired;
        };
    }
}
