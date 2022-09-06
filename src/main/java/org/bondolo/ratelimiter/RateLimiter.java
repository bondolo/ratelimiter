/*
 * Copyright © 2020 Mike Duigou
 */
package org.bondolo.ratelimiter;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * Simple bucket filling rate limiter
 * <p>
 * See <a href="https://blog.figma.com/an-alternative-approach-to-rate-limiting-f8a06cf7c94c">An Alternative Approach to Rate Limiting</a> for a description of the approach.
 *
 * @param <T> Type of bucket keys
 */
public final class RateLimiter<T> {

    /**
     * Tombstone used instead of null in bucket map
     */
    private static final Object NULL_BUCKET = new Object();

    /**
     * Our buckets of permits
     */
    private final ConcurrentMap<T, Bucket> buckets = new ConcurrentHashMap<>();

    /**
     * Nanosecond monotonic clock which governs how often we refill permits
     */
    final LongSupplier clock;

    /**
     * The number of permits per time interval per bucket
     */
    volatile int permits;

    /**
     * The maximum number of permits per bucket
     */
    volatile int limit;

    /**
     * The maximum number of permits per bucket
     */
    volatile int maxAcquire;

    /**
     * The time interval over which permits are refreshed in nanoseconds
     */
    volatile Duration interval;

    /**
     * The shared overall permit provider
     */
    final PermitProvider sharedLimit;

    /**
     * Establish a rate limiter for individual buckets with up to
     * {@link Integer#MAX_VALUE} permits. The system nanosecond monotonic clock,
     * {@link System#nanoTime()} is used as the clock.
     *
     * @param permits The number of permits per bucket per time interval
     * @param interval The time interval
     * @implNote The implementation doesn't track fractional permits. If you are
     * getting fewer permits than you expect use a longer interval and more
     * permits.
     */
    public RateLimiter(int permits, Duration interval) {
        this(Integer.MAX_VALUE, permits, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, interval, System::nanoTime);
    }

    /**
     * Establish a rate limiter for individual buckets
     *
     * @param maxAcquire    The maximum number of permits that can be acquired
     * @param permits       The number of permits per bucket per time interval
     * @param limit         The maximum number of permits a bucket can hold
     * @param sharedPermits The number of permits shared by all buckets per time
     *                      interval or Integer.MAX_VALUE for unlimited
     * @param sharedLimit   The maximum accumulated permits to be shared by all
     *                      buckets
     * @param interval      The time interval
     * @param clock         A monotonic nanosecond clock
     * @implNote The implementation doesn't track fractional permits. If you are
     * getting fewer permits than you expect use a longer interval and more
     * permits.
     * @implNote The current implementation overbalances "fairness" of
     * allocation of shared permits so that all buckets have access to permits
     * but does not allow a single bucket to use all the permits.
     */
    public RateLimiter(int maxAcquire, int permits, int limit, int sharedPermits, int sharedLimit, Duration interval, LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        // unchecked
        this.maxAcquire = maxAcquire;
        this.permits = permits;
        this.limit = limit;
        this.sharedLimit = sharedPermits == Integer.MAX_VALUE ? CORNUCOPIA : new ReservoirBucket(sharedPermits, sharedLimit);
        // checked
        setPermitRate(permits);
        setPermitMaxAcquire(maxAcquire);
        setPermitLimit(limit);
        setPermitInterval(interval);
    }

    @Override
    public String toString() {
        return String.format("RateLimiter{buckets=%d, %d:%d:%s, %s:%s:%s per %s", buckets.size(), maxAcquire, permits, Integer.MAX_VALUE == limit ? "∞" : Integer.toString(limit), Integer.MAX_VALUE == sharedLimit.getMaxAcquire() ? "∞" : Integer.toString(sharedLimit.getMaxAcquire()), Integer.MAX_VALUE == sharedLimit.getPermitRate() ? "∞" : Integer.toString(sharedLimit.getPermitRate()), Integer.MAX_VALUE == sharedLimit.getPermitLimit() ? "∞" : Integer.toString(sharedLimit.getPermitLimit()), interval);
    }

    /**
     * Gets the number of permits to be offered per bucket per time interval
     *
     * @return the number of permits to be offered per bucket per time interval
     */
    public int getPermitRate() {
        return permits;
    }

    /**
     * Sets the number of permits to be offered per bucket per time interval
     *
     * @param permits the number of permits to be offered per bucket per time
     *                interval
     * @return this instance
     * @implNote Increasing the permit rate does not wake threads already
     * waiting for permits
     */
    public RateLimiter<T> setPermitRate(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive and non-zero");
        }
        this.permits = permits;
        return this;
    }

    /**
     * Gets The maximum number of permits a bucket can hold
     *
     * @return the maximum number of permits a bucket can hold
     */
    public int getPermitLimit() {
        return limit;
    }

    /**
     * Gets the maximum number of permits per single acquisition
     *
     * @return the maximum number of permits per single acquisition
     */
    public int getPermitMaxAcquire() {
        return maxAcquire;
    }

    /**
     * Gets the maximum number of permits per single acquisition
     *
     * @param maxAcquire the maximum number of permits per single acquisition
     * @return the maximum number of permits per single acquisition
     */
    public RateLimiter<T> setPermitMaxAcquire(int maxAcquire) {
        if (maxAcquire <= 0 || maxAcquire > limit) {
            throw new IllegalArgumentException("max acquire must be positive and non-zero and less than or equal to limit");
        }
        this.maxAcquire = maxAcquire;
        return this;
    }

    /**
     * Sets the number of permits to be offered per bucket per time interval
     *
     * @param limit The maximum number of permits a bucket can hold
     * @return this instance
     * @implNote Increasing the permit rate does not wake threads already
     * waiting for permits
     */
    public RateLimiter<T> setPermitLimit(int limit) {
        if (limit <= 0 || maxAcquire > limit) {
            throw new IllegalArgumentException("limit must be positive and non-zero and greater than or equal to permits and max request");
        }
        this.limit = limit;
        return this;
    }

    /**
     * Gets the time interval over which additional permits are issued in
     * nanoseconds
     *
     * @return the time interval over which additional permits are issued in
     * nanoseconds
     * @implNote Decreasing the permit interval does not wake threads already
     * waiting for permits
     */
    public Duration getPermitInterval() {
        return interval;
    }

    /**
     * Sets the time interval over which additional permits are issued
     *
     * @param interval     The time interval
     * @return this instance
     */
    public RateLimiter<T> setPermitInterval(Duration interval) {
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be positive and non-zero");
        }
        this.interval = Objects.requireNonNull(interval, "Duration");
        return this;
    }

    /**
     * Acquire from a bucket a single permit, blocking until available if
     * necessary
     *
     * @param bucket The bucket from which the permit shall be acquired
     * @throws java.lang.InterruptedException if interrupted while waiting for a
     *                                        permit
     */
    public void acquire(T bucket) throws InterruptedException {
        acquire(bucket, 1);
    }

    /**
     * Returns the count of permits currently available for a bucket, creating
     * the bucket if necessary.
     *
     * @param bucket The bucket of interest
     * @return The number of permits currently available
     */
    @SuppressWarnings("unchecked")
    public int available(T bucket) {
        return buckets.computeIfAbsent(null != bucket ? bucket : (T) NULL_BUCKET, k -> new TaggedBucket()).availablePermits();
    }

    /**
     * Acquire from a bucket permits, blocking until available if necessary
     *
     * @param bucket  The bucket from which the permits shall be acquired
     * @param permits The number of permits to acquire
     * @throws java.lang.InterruptedException if interrupted while waiting for
     *                                        permits
     */
    @SuppressWarnings("unchecked")
    public void acquire(T bucket, int permits) throws InterruptedException {
        buckets.computeIfAbsent(null != bucket ? bucket : (T) NULL_BUCKET, k -> new TaggedBucket()).acquirePermits(permits);
    }

    /**
     * Acquire from a bucket a single permit if available
     *
     * @param bucket The bucket from which the permit shall be acquired
     * @return true if permit was available
     */
    public boolean tryAcquire(T bucket) {
        return tryAcquire(bucket, 1);
    }

    /**
     * Acquire from a bucket permits if available
     *
     * @param bucket  The bucket from which the permits shall be acquired
     * @param permits The number of permits to acquire
     * @return true if permit was available
     */
    @SuppressWarnings("unchecked")
    public boolean tryAcquire(T bucket, int permits) {
        return buckets.computeIfAbsent(null != bucket ? bucket : (T) NULL_BUCKET, k -> new TaggedBucket()).tryAcquirePermits(permits);
    }

    /**
     * Forgets about the specified bucket and permits currently allocated to it.
     * Any permits held by the bucket are discarded.
     *
     * @param bucket The bucket to forget
     * @return true if the bucket existed otherwise false
     */
    @SuppressWarnings({"element-type-mismatch"})
    public boolean remove(T bucket) {
        return null != buckets.remove(null != bucket ? bucket : NULL_BUCKET);
    }

    /**
     * Returns a stream of the buckets known to this rate limiter
     *
     * @return a stream of the buckets known to this rate limiter
     */
    public Stream<T> buckets() {
        return buckets.keySet().stream().map(t -> NULL_BUCKET != t ? t : null);
    }

    /**
     * Returns the count of permits claimed for the specified bucket
     *
     * @param bucket The bucket of interest
     * @return the count of permits claimed for the specified bucket. Always
     * zero for untracked buckets.
     */
    @SuppressWarnings({"element-type-mismatch"})
    public long claimed(T bucket) {
        Bucket b = buckets.get(null != bucket ? bucket : NULL_BUCKET);
        return (null != b ? b.claimed.longValue() : 0);
    }

    /**
     * Returns the count of permits denied for the specified bucket
     *
     * @param bucket The bucket of interest
     * @return the count of permits denied for the specified bucket. Always zero
     * for untracked buckets.
     */
    @SuppressWarnings({"element-type-mismatch"})
    public long denied(T bucket) {
        Bucket b = buckets.get(null != bucket ? bucket : NULL_BUCKET);
        return (null != b ? b.denied.longValue() : 0);
    }

    /**
     * A source of permits
     */
    private interface PermitProvider {

        /**
         * Returns the maximum number of permits, {@code 1-Integer.MAX_VALUE},
         * that may be acquired
         *
         * @return the maximum number of permits that may be acquired
         */
        int getMaxAcquire();

        /**
         * Gets the number of permits to be offered per bucket per time interval
         *
         * @return the number of permits to be offered per bucket per time
         * interval
         */
        int getPermitRate();

        /**
         * Gets The maximum number of permits a bucket can hold
         *
         * @return the maximum number of permits a bucket can hold
         */
        int getPermitLimit();

        /**
         * Gets the time interval over which additional permits are issued in
         * nanoseconds
         *
         * @return the time interval over which additional permits are issued in
         * nanoseconds
         * @implNote Decreasing the permit interval does not wake threads
         * already waiting for permits
         */
        Duration getPermitInterval();

        /**
         * Get the count of available permits
         *
         * @return The available permits, possibly zero
         */
        int availablePermits();

        /**
         * Acquire permits waiting it necessary
         *
         * @param permits The requested number of permits
         * @throws IllegalArgumentException if permits is less than or equal to
         *                                  zero or exceeds the maximum number of acquirable permits
         * @throws InterruptedException     if thread is interrupted while waiting
         *                                  for permits
         * @apiNote Be sure to catch the IllegalArgumentException if requesting
         * more than one permit as dynamic reductions in the maximum number of
         * acquirable permits may cause unexpected IllegalArgumentException.
         */
        void acquirePermits(int permits) throws InterruptedException;

        /**
         * Acquire available permits
         *
         * @param permits The requested number of permits
         * @return The available permits, possibly zero
         * @throws IllegalArgumentException if permits is less than or equal to
         *                                  zero
         */
        int acquireAvailablePermits(int permits);

        /**
         * Attempt to acquire permits if available
         *
         * @param permits The requested number of permits
         * @return true if permits were available
         * @throws IllegalArgumentException if permits is less than or equal to
         *                                  zero
         */
        boolean tryAcquirePermits(int permits);
    }

    private static final int EMA_WIDTH = 10;

    private static abstract class AbstractPermitProvider implements PermitProvider {
        ExponentialMovingAverage permitsPerInterval = new ExponentialMovingAverage(EMA_WIDTH);
        ExponentialMovingAverage refillInterval = new ExponentialMovingAverage(EMA_WIDTH);

        @Override
        public abstract int getMaxAcquire();

        @Override
        public abstract int getPermitRate();

        @Override
        public abstract int getPermitLimit();

        @Override
        public abstract Duration getPermitInterval();

        @Override
        public abstract int availablePermits();

        @Override
        public void acquirePermits(int permits) throws InterruptedException {

        }

        @Override
        public int acquireAvailablePermits(int permits) {
            return 0;
        }

        @Override
        public boolean tryAcquirePermits(int permits) {
            return false;
        }
    }

    /**
     * An inexhaustible unlimited source of permits
     */
    private static final PermitProvider CORNUCOPIA = new PermitProvider() {

        @Override
        public int getPermitRate() {
            return Integer.MAX_VALUE;
        }

        @Override
        public Duration getPermitInterval() {
            return Duration.ofNanos(1);
        }

        @Override
        public int getPermitLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getMaxAcquire() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int availablePermits() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void acquirePermits(int permits) {
            if (permits < 0) {
                throw new IllegalArgumentException("invalid permit request " + permits);
            }
        }

        @Override
        public int acquireAvailablePermits(int permits) {
            if (permits < 0) {
                throw new IllegalArgumentException("invalid permit request " + permits);
            }
            return permits;
        }

        @Override
        public boolean tryAcquirePermits(int permits) {
            if (permits < 0) {
                throw new IllegalArgumentException("invalid permit request " + permits);
            }
            return true;
        }
    };

    /**
     * A bucket for storing the permits
     */
    private abstract static class Bucket implements PermitProvider {

        /**
         * Permit bucket
         */
        protected final Semaphore bucket;

        /**
         * clock time at which permits were last added
         */
        protected final AtomicLong lastFill;

        /**
         * The total count of permits acquired
         */
        protected final LongAdder claimed = new LongAdder();

        /**
         * The total count of permits denied
         */
        protected final LongAdder denied = new LongAdder();

        @SuppressWarnings("OverridableMethodCallInConstructor")
        Bucket() {
            // Start the bucket with one time-unit worth of permits
            bucket = new Semaphore(getPermitRate());
            lastFill = new AtomicLong(clockNow());
        }

        @Override
        public String toString() {
            Duration elapsed = Duration.ofNanos(clockNow() - lastFill.longValue());

            return String.format("%s{permits=%d, claimed=%d, denied=%s, sinceFill=%s}", getClass().getSimpleName(), bucket.availablePermits(), claimed.longValue(), denied.longValue(), elapsed);
        }

        /**
         * Returns the current time using the preferred nanosecond clock
         *
         * @return the current time using the preferred nanosecond clock
         */
        protected abstract long clockNow();

        /**
         * Returns the source of permits for this bucket
         *
         * @return the source of permits for this bucket
         */
        protected abstract PermitProvider permitSource();

        @Override
        public int availablePermits() {
            return bucket.availablePermits();
        }

        @Override
        public void acquirePermits(int permits) throws InterruptedException {
            boolean acquired;
            do {
                if (permits <= 0 || permits > getMaxAcquire()) {
                    throw new IllegalArgumentException("invalid permit request " + permits);
                }
                // optimistically attempt
                acquired = bucket.tryAcquire(permits);

                while (!acquired) {
                    acquired = permits == refill(permits, true);
                }
            } while (!acquired);

            claimed.add(permits);
        }

        @Override
        public int acquireAvailablePermits(int permits) {
            permits = Math.min(getMaxAcquire(), permits);
            //noinspection CatchMayIgnoreException
            try {
                refill(0, false);
            } catch (InterruptedException never) {
            }
            int attempt = Math.min(bucket.availablePermits(), permits);
            int acquired = bucket.tryAcquire(attempt) ? attempt : 0;

            claimed.add(acquired);

            return acquired;
        }

        @Override
        public boolean tryAcquirePermits(int permits) {
            boolean acquired;

            if (permits <= 0) {
                throw new IllegalArgumentException("invalid permit request " + permits);
            }

            if (permits > getMaxAcquire()) {
                denied.add(permits);
                return false;
            }

            // optimistically attempt
            acquired = bucket.tryAcquire(permits);

            if (!acquired) //noinspection CatchMayIgnoreException
                try {
                acquired = permits == refill(permits, false);
            } catch (InterruptedException never) {
            }

            (acquired ? claimed : denied).add(permits);

            return acquired;
        }

        /**
         * Refill the bucket with permits
         *
         * @param permits Number of permits desired
         * @param wait    if true then wait for permits to be available
         * @return The number of permits
         * @throws InterruptedException if interrupted while waiting.
         */
        protected int refill(int permits, boolean wait) throws InterruptedException {
            // attempt a refill
            long lastFilled = lastFill.longValue();
            long nowNanos = clockNow();
            Duration elapsed = Duration.ofNanos(nowNanos - lastFilled);
            int refillPermits = (int) Math.min(Integer.MAX_VALUE, elapsed.toNanos() * getPermitRate() / getPermitInterval().toNanos());
            if (0 == refillPermits) {
                if (wait) {
                    // none available, wait until needed permits would appear
                    long await = (permits - bucket.availablePermits()) * getPermitInterval().toNanos() / getPermitRate();
                    boolean acquired = bucket.tryAcquire(permits, await, TimeUnit.NANOSECONDS);
                    if (!acquired) {
                        // try again to refill
                        permits = 0;
                    }
                } else {
                    // no permits to be had, give up
                    permits = 0;
                }
            } else {
                refillPermits = permitSource().acquireAvailablePermits(refillPermits);

                if (refillPermits < permits) {
                    permits = 0;
                } else {
                    // filler gets priority on the fresh permits
                    refillPermits -= permits;
                }

                if (lastFill.compareAndSet(lastFilled, nowNanos)) {
                    bucket.release(Math.min(getPermitLimit() - bucket.availablePermits(), refillPermits));
                } else permits = bucket.tryAcquire(permits) ? permits : 0;
            }
            return permits;
        }
    }

    /**
     * Individual bucket which gets source and limits from owning RateLimiter
     * instance
     */
    class TaggedBucket extends Bucket {

        @Override
        protected final long clockNow() {
            return RateLimiter.this.clock.getAsLong();
        }

        @Override
        protected final PermitProvider permitSource() {
            return RateLimiter.this.sharedLimit;
        }

        @Override
        public final int getPermitRate() {
            return RateLimiter.this.permits;
        }

        @Override
        public final Duration getPermitInterval() {
            return RateLimiter.this.interval;
        }

        @Override
        public final int getPermitLimit() {
            return RateLimiter.this.limit;
        }

        @Override
        public final int getMaxAcquire() {
            return RateLimiter.this.maxAcquire;
        }
    }

    /**
     * Shared rate limit permit source
     */
    class ReservoirBucket extends Bucket {

        private final int permits;

        private final int limit;

        ReservoirBucket(int permits, int limit) {
            if (permits <= 0) {
                throw new IllegalArgumentException("permits must be positive and non-zero");
            }

            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive and non-zero");
            }

            this.permits = permits;
            this.limit = limit;
        }

        @Override
        protected final long clockNow() {
            return RateLimiter.this.clock.getAsLong();
        }

        @Override
        protected final PermitProvider permitSource() {
            return CORNUCOPIA;
        }

        @Override
        public final int getPermitRate() {
            return permits;
        }

        @Override
        public final Duration getPermitInterval() {
            return RateLimiter.this.interval;
        }

        @Override
        public final int getPermitLimit() {
            return limit;
        }

        @Override
        public final int getMaxAcquire() {
            int available = bucket.availablePermits();
            if (available < getPermitRate()) //noinspection CatchMayIgnoreException
                try {
                // refill before deciding how much is available
                refill(0, false);
                available = bucket.availablePermits();
            } catch (InterruptedException never) {
            }

            return Math.max(1, (available > getPermitRate() ? getPermitRate() : available) / Math.max(1, RateLimiter.this.buckets.size()));
        }

        @Override
        protected final int refill(int permits, boolean wait) throws InterruptedException {
            int before = bucket.availablePermits();

            permits = super.refill(permits, wait);

            int after = bucket.availablePermits();

            if (after > before) {
                // We just refilled. remove buckets that are not being actively used
                Duration tilSharedLimit = RateLimiter.this.interval.multipliedBy(limit / RateLimiter.this.permits);
                RateLimiter.this.buckets.values().removeIf((RateLimiter.Bucket b) -> {
                    Duration elapsed = Duration.ofNanos(b.clockNow() - b.lastFill.longValue());
                    // remove if eligible refill is greater than shared limit and has not been denied
                    return 0 == b.denied.longValue() && elapsed.compareTo(tilSharedLimit) >= 1;
                });
            }
            return permits;
        }
    }
}
