package org.bondolo.ratelimiter;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TimeBasedMovingAverageTest {

    @Test
    void testToString() {
        var avg = new TimeBasedMovingAverage(Duration.ofSeconds(1));
        assertThat(avg).hasToString(Double.toString(Double.NaN));
        avg.add(1.0);
        assertThat(avg).hasToString(Double.toString(1.0));
    }

    @Test
    void add() {
    }

    @Test
    void testInitialAdd() {
        final var interval = Duration.ofSeconds(1);
        var avg = new TimeBasedMovingAverage(interval);
        double average = avg.add(1.0, Instant.ofEpochMilli(0));
        assertThat(average).isEqualTo(1.0);
        average = avg.add(2.0, Instant.ofEpochMilli(interval.toMillis()));
        assertThat(average).isCloseTo(1.5, Percentage.withPercentage(0.1));
    }

    @Test
    void testInitialAdds() {
        final var interval = Duration.ofSeconds(1);
        var avg = new TimeBasedMovingAverage(interval);
        double average = Double.NaN;
        for (long tick = 0; tick < interval.toMillis(); tick++) {
            average = avg.add(tick, Instant.ofEpochMilli(tick));
            System.out.printf("tick: %d avg: %g\n", tick, average);
        }
        assertThat(average).isCloseTo(500.0, Percentage.withPercentage(0.1));
    }

    @Test
    void testAfterInitialAdds() {
        final var interval = Duration.ofSeconds(1);
        var avg = new TimeBasedMovingAverage(interval);
        double average = Double.NaN;
        for (long tick = 0; tick < interval.toMillis(); tick++) {
            average = avg.add(tick, Instant.ofEpochSecond(tick));
            System.out.printf("tick: %d avg: %g\n", tick, average);
        }
        assertThat(average).isCloseTo(500.0, Percentage.withPercentage(0.1));
    }

    @Test
    void testAdd1() {
    }

    @Test
    void testAdd2() {
    }

    @Test
    void isValid() {
        var avg = new TimeBasedMovingAverage(Duration.ofSeconds(1));
        assertThat(avg.isValid()).isFalse();
        avg.add(1.0);
        assertThat(avg.isValid()).isTrue();
    }

    @Test
    void reset() {
        var avg = new TimeBasedMovingAverage(Duration.ofSeconds(1));
        assertThat(avg.isValid()).isFalse();
        avg.add(1.0);
        assertThat(avg.isValid()).isTrue();
        avg.reset();
        assertThat(avg.isValid()).isFalse();
    }
}