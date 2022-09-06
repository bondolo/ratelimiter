/*
 * Copyright © 2020 Mike Duigou
 */
package org.bondolo.ratelimiter;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Performs a moving average over a specified interval. New observations are weighted based on when they occur relative
 * to the most recent observation.
 *
 * @implNote Not thread safe
 * @implNote Assumes that observed values are of similar scale. If the observed
 * values are of vastly different scales the result will be inaccurate.
 */
public class TimeBasedMovingAverage {

    /**
     * The interval over which
     */
    protected final Duration interval;

    protected final Duration halfInterval;

    /**
     * The average of the observed values. Always
     *
     * ({@link #baseValue} + {@link #lastValue}) / 2
     */
    protected double average;

    protected double lastValue;

    /**
     * computed value for the base
     */
    protected double baseValue;

    /**
     * The time base for the current average
     */
    @Nullable
    protected Instant base;

    /**
     * The time of the last value
     */
    @Nullable Instant last;

    /**
     * The clock used for computing the current instant.
     */
    protected Clock clock;

    /**
     * Creates a moving average for the provided interval. The standard {@link Clock#systemUTC()} clock will be used.
     *
     * @param interval The interval over which the average will derived.
     */
    public TimeBasedMovingAverage(Duration interval) {
        this(interval, Double.NaN, null, Clock.systemUTC());
    }

    /**
     * Creates a moving average for the provided interval.
     *
     * @param interval The interval over which the average will derived.
     * @param average  initial value for the average
     * @param base     The base time for the average
     * @param clock    monotonic and hopefully continuous.
     */
    protected TimeBasedMovingAverage(Duration interval, double average, @Nullable Instant base, Clock clock) {
        if (Duration.ZERO.compareTo(interval) >= 0) {
            throw new IllegalArgumentException("interval must be positive non-zero");
        }
        this.interval = interval;
        this.halfInterval = interval.dividedBy(2);
        if (Duration.ZERO.compareTo(halfInterval) >= 0) {
            throw new IllegalArgumentException("interval must be at least two nanoseconds.");
        }
        this.average = average;
        this.base = base;
        this.clock = clock;
    }

    @Override
    public String toString() {
        return Double.toString(average);
    }

    /**
     * Add an observed value at the current time.
     *
     * @param v the value, ignored if NaN.
     * @return the current average
     */
    public double add(double v) {
        return add(v, clock.instant());
    }

    /**
     * Add an observed value.
     *
     * <p>The value is either:
     * - the first value.
     *  - base time is set to the value time
     *  - last time is set to the value time
     *  - base value is set to the value
     *  - last value is set to the value
     *  - average is set to the value
     * - less than the interval past the base time
     *   - last value is set to the value
     *   - last time is set to the value time
     *   - average is calculated based upon ratio of base to last time and last time to value time.
     * - less than the interval past the last time
     *  - new base time is calculated to value time minus interval
     *  - new base value is computed using prior base and last values
     *  - last time is set to the value time
     *  - last value is set to the value
     *  - average is computed from base value and last value
     * - more than the interval past the last value
     *  - new base time is calculated to value time minus interval
     *  - new base value is computed using previous average at half of interval
     * @param v the value, ignored if NaN.
     * @param when The instant at which the value was observed. Must use the same clock as provided by the constructor.
     * @return the current average
     * @throws IllegalArgumentException if the provided value is earlier than prior values.
     */
    public double add(double v, Instant when) {

        var a = average;
        if(Double.isNaN(v)) return a;
        if (null == last) {
            base = last = when;
            return average = lastValue = baseValue = v;
        }

        Duration sinceLast = Duration.between(last, when);
        if (sinceLast.isNegative()) {
            throw new IllegalArgumentException( when + " is before " + last);
        }

        Instant newBase = when.minus(interval);
        double newBaseValue;
        if (base.compareTo(newBase) >= 0) {
            // base won't be moving, but we need to update average.
            double ratio;
            if (last.isAfter(base)) {
                Duration beforeLast = Duration.between(base, last);
                Duration afterLast = Duration.between(last, when);
                long beforeNanos = beforeLast.toNanos();
                long afterNanos = afterLast.toNanos();
                ratio = beforeNanos > 0 ? 1.0 - (afterNanos / (double) beforeNanos) : 0.0;
                System.out.printf("ratio: %g\n", ratio);
            } else {
                // there has only been one prior value.
                ratio = 0.5;
            }
            last = when;
            lastValue = v;
            return average = ((average * ratio) + (v * (1 - ratio)));
        } else if (sinceLast.compareTo(interval) <= 0) {
            // inside of interval
            Duration beforeNewBase = Duration.between(base, newBase);
            Duration afterNewBase = Duration.between(newBase, last);
            long beforeNanos = beforeNewBase.toNanos();
            long afterNanos = afterNewBase.toNanos();
            double ratio = afterNanos > 0 ? beforeNanos / afterNanos : 1.0;
            System.out.printf("inside ratio: %g\n", ratio);
            newBaseValue = (baseValue + lastValue) * ratio;
        } else {
            // outside of interval
            Instant oldAverageAt = last.minus(halfInterval);
            Duration beforeNewBase = Duration.between(oldAverageAt, newBase);
            Duration afterNewBase = Duration.between(newBase, when);
            long beforeNanos = beforeNewBase.toNanos();
            long afterNanos = afterNewBase.toNanos();
            double ratio = afterNanos > 0 ? beforeNanos / afterNanos : 1.0;
            System.out.printf("outside ratio: %g\n", ratio);
            newBaseValue = (average + v) * ratio;
        }
        base = newBase;
        baseValue = newBaseValue;
        last = when;
        lastValue = v;
        return average = (newBaseValue + v ) / 2.0;
    }

    /**
     * Add an observed value
     *
     * @param v the value, ignored if null or NaN
     * @return the current average
     */
    public double add(Double v) {
        return v == null ? average : add(v.doubleValue(), clock.instant());
    }

    /**
     * Add an observed value
     *
     * @param v the value, ignored if null or NaN
     * @return the current average
     */
    public double add(Number v) {
        return v == null ? average : add(v.doubleValue(), clock.instant());
    }

    /**
     * return the current average or NaN if no observed values
     *
     * @return the current average
     */
    public double average() {
        return average;
    }

    /**
     * Returns true if this moving average has a value
     *
     * @return true if this moving average has a value
     */
    public boolean isValid() {
        return !Double.isNaN(average);
    }

    /**
     * Reset the average
     */
    public void reset() {
        base = last = null;
        baseValue = lastValue = average = Double.NaN;
    }

//    /**
//     * Combine this moving average with another moving average producing a new
//     * moving average instance
//     *
//     * @param other the other moving average
//     * @return The combined moving average as a new instance.
//     */
//    public TimeBasedMovingAverage combine(TimeBasedMovingAverage other) {
//        double combinedAverage = isValid()
//                ? other.isValid()
//                ? average == other.average
//                ? average
//                // combination of averages is weighted by counts for each
//                : (average * (Math.min(count, interval) / (double) Math.min(other.count, other.interval))
//                + other.average * (Math.min(other.count, other.interval) / (double) Math.min(count, interval))) / 2.0
//                : average
//                : other.isValid()
//                ? other.average
//                : Double.NaN; // neither valid
//        int combinedWidth = interval == other.interval ? interval : (interval + other.interval) / 2;
//        long combinedCount = count + other.count;
//        return new TimeBasedMovingAverage(combinedAverage, combinedWidth, combinedCount);
//    }
}
