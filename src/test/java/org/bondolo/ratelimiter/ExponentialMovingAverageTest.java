package org.bondolo.ratelimiter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Basic tests for class ExponentialMovingAverageTest
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ExponentialMovingAverageTest {

    /**
     * Test of toString method, of class ExponentialMovingAverage.
     */
    @Test
    void testToString() {
        System.out.println("toString");
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        String expResult = "NaN";
        String result = instance.toString();
        assertEquals(expResult, result);
        expResult = "1.0";
        instance.add(1.0);
        result = instance.toString();
        assertEquals(expResult, result);
    }

    /**
     * Test of add method, of class ExponentialMovingAverage.
     */
    @Test
    void testAdd_double() {
        System.out.println("add");
        double v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = 0.0;
        double result = instance.add(v);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of add method, of class ExponentialMovingAverage.
     */
    @Test
    void testAdd_Double() {
        System.out.println("add");
        Double v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = 0.0;
        double result = instance.add(v);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of add method, of class ExponentialMovingAverage.
     */
    @Test
    void testAdd_Number() {
        System.out.println("add");
        Number v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = 0.0;
        double result = instance.add(v);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of get method, of class ExponentialMovingAverage.
     */
    @Test
    void testGet() {
        System.out.println("get");
        Number v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = 0.0;
        double result = instance.add(v);
        assertEquals(expResult, result, 0.0);
        assertEquals(expResult, instance.average(), 0.0);
    }

    /**
     * Test of reset method, of class ExponentialMovingAverage.
     */
    @Test
    void testReset() {
        System.out.println("reset");
        double v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = Double.NaN;
        double result = instance.average();
        assertEquals(expResult, result, 0.0);
        expResult = 0.0;
        result = instance.add(v);
        assertEquals(expResult, result, 0.0);
        expResult = Double.NaN;
        instance.reset();
        result = instance.average();
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of combine method, of class ExponentialMovingAverage.
     */
    @Test
    void testCombine() {
        System.out.println("combine");
        ExponentialMovingAverage other = new ExponentialMovingAverage(10);
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        double expResult = 0.5f;
        other.add(0.0);
        instance.add(1.0);
        ExponentialMovingAverage result = instance.combine(other);
        assertEquals(expResult, result.average(), 0.0);
    }

    /**
     * Test of isValid method, of class ExponentialMovingAverage.
     */
    @Test
    void testIsValid() {
        System.out.println("isValid");
        double v = 0.0;
        ExponentialMovingAverage instance = new ExponentialMovingAverage(10);
        boolean result = instance.isValid();
        assertFalse(result);
        instance.add(v);
        result = instance.isValid();
        assertTrue(result);
        instance.reset();
        result = instance.isValid();
        assertFalse(result);
    }

}