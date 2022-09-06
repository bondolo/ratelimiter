/*
 * Copyright © 2020 Mike Duigou
 */
package org.bondolo.ratelimiter;

/**
 * @see
 * <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">Wikipedia</a>
 *
 * <p>Until the count of observed values exceeds the provided width the average
 * will be weighted by the count of observed values.
 *
 * @implNote Not thread safe
 * @implNote Assumes that observed values are of similar scale. If the observed
 * values are of vastly different scales the result will be inaccurate.
 */
public class ExponentialMovingAverage {

    /**
     * The count of observed values over which averaging will be performed.
     * Affects the {@link #weight}
     */
    protected final int width;

    /**
     * Weight of each observed value in determining average. 1 / "width"
     */
    protected final double weight;

    /**
     * The average of the observed values
     */
    protected double average;

    /**
     * The count of observed values
     */
    protected long count;

    /**
     * Creates an EMA parameterized like a <i>Modified moving average</i>, see
     * Wikipedia for details.
     *
     * @param width The count of values over which the average will derived.
     * The larger the width, the lower the contribution of each value to the
     * average once the count of observed values reaches the width.
     */
    public ExponentialMovingAverage(int width) {
        this(Double.NaN, width, 0);
    }

    /**
     * Creates an EMA parameterized like a <i>Modified moving average</i>, see
     * Wikipedia for details.
     *
     * @param average initial value for the average
     * @param width The count of values over which the average will derived.
     * The larger the width, the lower the contribution of each value to the
     * average once the count of observed values reaches the width.
     * @param count the count of observed values to be assumed
     */
    protected ExponentialMovingAverage(double average, int width, long count) {
        if (width <= 0) {
            throw new IllegalArgumentException("width must be positive non-zero");
        }
        this.width = width;
        this.weight = 1.0 / width;
        this.average = average;
        this.count = count;
    }

    @Override
    public String toString() {
        return Double.toString(average);
    }

    /**
     * Add an observed value
     *
     * @param v the value, ignored if NaN.
     * @return the current average
     */
    public double add(double v) {
        double a = average;
        if(Double.isNaN(v)) return a;
        count++;
        if(Double.isNaN(a)) return average = v;
        //            return average = v * w + a * (1 - w);
        double adjustment = (v - a) * (count > width ? weight : 1.0 / count);
        return average = a + adjustment;
    }

    /**
     * Add an observed value
     *
     * @param v the value, ignored if null or NaN
     * @return the current average
     */
    public double add(Double v) {
        return v == null ? average : add(v.doubleValue());
    }

    /**
     * Add an observed value
     *
     * @param v the value, ignored if null or NaN
     * @return the current average
     */
    public double add(Number v) {
        return v == null ? average : add(v.doubleValue());
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
        count = 0;
        average = Double.NaN;
    }

    /**
     * Combine this moving average with another moving average producing a new
     * moving average instance
     *
     * @param other the other moving average
     * @return The combined moving average as a new instance.
     */
    public ExponentialMovingAverage combine(ExponentialMovingAverage other) {
        double combinedAverage = isValid()
                ? other.isValid()
                ? average == other.average
                ? average
                // combination of averages is weighted by counts for each
                : (average * (Math.min(count, width) / (double) Math.min(other.count, other.width))
                + other.average * (Math.min(other.count, other.width) / (double) Math.min(count, width))) / 2.0
                : average
                : other.isValid()
                ? other.average
                : Double.NaN; // neither valid
        int combinedWidth = width == other.width ? width : (width + other.width) / 2;
        long combinedCount = count + other.count;
        return new ExponentialMovingAverage(combinedAverage, combinedWidth, combinedCount);
    }
}
