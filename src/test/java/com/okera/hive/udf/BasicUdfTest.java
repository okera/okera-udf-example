package com.okera.hive.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BasicUdfTest {

  @Test
  public void testMask() {
    MaskUDF udf = new MaskUDF();
    assertTrue(udf.evaluate(null) == null);
    assertEquals("", udf.evaluate(new Text("")).toString());
    assertEquals("xxxx", udf.evaluate(new Text("abcd")).toString());
    assertEquals("xxxxxxxxxx", udf.evaluate(new Text("helloworld")).toString());
  }

  @Test
  public void testMaskCcn() {
    MaskCcnUDF udf = new MaskCcnUDF();
    assertTrue(udf.evaluate(null) == null);
    assertEquals("", udf.evaluate(new Text("")).toString());
    assertEquals("ab", udf.evaluate(new Text("ab")).toString());
    assertEquals("abcd", udf.evaluate(new Text("abcd")).toString());
    assertEquals("XXXXXXorld", udf.evaluate(new Text("helloworld")).toString());
    assertEquals("XXXXXXXXXXXX4444",
        udf.evaluate(new Text("1111222233334444")).toString());
    assertEquals("XXXX-XXXX-XXXX-4444",
        udf.evaluate(new Text("1111-2222-3333-4444")).toString());
  }

  @Test
  public void nullTest() {
    NullUDF udf = new NullUDF();
    assertTrue(udf.evaluate((Text)null) == null);
    assertTrue(udf.evaluate(new Text("a")) == null);

    assertTrue(udf.evaluate(new BooleanWritable(false)) == null);
    assertTrue(udf.evaluate(new ByteWritable((byte)0)) == null);
    assertTrue(udf.evaluate(new ShortWritable((short)1)) == null);
    assertTrue(udf.evaluate(new IntWritable(2)) == null);
    assertTrue(udf.evaluate(new LongWritable(3)) == null);
    assertTrue(udf.evaluate(new FloatWritable(4)) == null);
    assertTrue(udf.evaluate(new DoubleWritable(5)) == null);
    assertTrue(udf.evaluate(new DateWritable(6)) == null);
    assertTrue(udf.evaluate(new TimestampWritable()) == null);
    assertTrue(udf.evaluate(new HiveDecimalWritable(0)) == null);
  }

  @Test
  public void zeroTest() {
    ZeroUDF udf = new ZeroUDF();
    assertEquals("", udf.evaluate(new Text("")).toString());
    assertEquals("", udf.evaluate(new Text("ab")).toString());
    assertEquals("", udf.evaluate(new Text("abcd")).toString());

    assertEquals(0, udf.evaluate(new LongWritable(0)).get());
    assertEquals(0, udf.evaluate(new LongWritable(123)).get());
    assertEquals(0, udf.evaluate(new LongWritable(-123)).get());

    assertEquals(0, udf.evaluate(new DoubleWritable(0)).get(), 0);
    assertEquals(0, udf.evaluate(new DoubleWritable(123)).get(), 0);
    assertEquals(0, udf.evaluate(new DoubleWritable(-123)).get(), 0);

    assertEquals("0", udf.evaluate(new HiveDecimalWritable(0)).toString());
    assertEquals("0", udf.evaluate(new HiveDecimalWritable(123)).toString());
    assertEquals("0", udf.evaluate(new HiveDecimalWritable(-123)).toString());

    assertEquals("1970-01-01", udf.evaluate(new DateWritable(0)).toString());
    assertEquals("1970-01-01", udf.evaluate(new DateWritable(123)).toString());
    assertEquals("1970-01-01", udf.evaluate(new DateWritable(-123)).toString());

    assertEquals("1969-12-31 16:00:00",
        udf.evaluate(new TimestampWritable()).toString());
    assertEquals("1969-12-31 16:00:00",
        udf.evaluate(new TimestampWritable(new Timestamp(123))).toString());
    assertEquals("1969-12-31 16:00:00",
        udf.evaluate(new TimestampWritable(new Timestamp(-123))).toString());
  }

  @Test
  public void testZip3() {
    PhiZip3UDF udf = new PhiZip3UDF();
    assertTrue(udf.evaluate((Text)null) == null);
    assertEquals("", udf.evaluate(new Text("")).toString());
    assertEquals("012", udf.evaluate(new Text("12")).toString());
    assertEquals("123", udf.evaluate(new Text("1234")).toString());
    assertEquals("941", udf.evaluate(new Text("94117")).toString());
    assertEquals("000", udf.evaluate(new Text("03611")).toString());
  }

  @Test
  public void testDobDate() {
    PhiDobUDF udf = new PhiDobUDF();
    assertTrue(udf.evaluate((DateWritable)null) == null);
    assertEquals("1970-01-01",
        udf.evaluate(new DateWritable(0)).toString());
    // I hope this code base is good in 140 years
    assertEquals("2019-01-01",
        udf.evaluate(new DateWritable(50 * 365)).toString());
    assertEquals("2469-01-01",
        udf.evaluate(new DateWritable(500 * 365)).toString());
    // This is the value if there is no clamping.
    assertFalse("1870-01-25".equals(
        udf.evaluate(new DateWritable(-100 * 365)).toString()));
    assertFalse("1870-01-01".equals(
        udf.evaluate(new DateWritable(-100 * 365)).toString()));
  }

  @Test
  public void testDate() {
    PhiDateUDF udf = new PhiDateUDF();
    assertTrue(udf.evaluate((DateWritable)null) == null);
    assertEquals("1970-01-01",
        udf.evaluate(new DateWritable(0)).toString());
    assertEquals("2019-01-01",
        udf.evaluate(new DateWritable(50 * 365)).toString());
    assertEquals("2469-01-01",
        udf.evaluate(new DateWritable(500 * 365)).toString());
    assertEquals("1870-01-01",
        udf.evaluate(new DateWritable(-100 * 365)).toString());
  }

  @Test
  public void testAge() {
    PhiAgeUDF udf = new PhiAgeUDF();
    assertTrue(udf.evaluate((IntWritable)null) == null);
    assertEquals(0, udf.evaluate(new IntWritable(0)).get());
    assertEquals(50, udf.evaluate(new IntWritable(50)).get());
    assertEquals(90, udf.evaluate(new IntWritable(90)).get());
    assertEquals(90, udf.evaluate(new IntWritable(100)).get());
    assertEquals(-50, udf.evaluate(new IntWritable(-50)).get());
    assertEquals(-150, udf.evaluate(new IntWritable(-150)).get());
  }

  @Test
  public void testSetsIntersect() {
    SetsIntersectUDF udf = new SetsIntersectUDF();
    assertTrue(udf.evaluate(null, null).get() == false);
    assertTrue(udf.evaluate(new Text("a,b,c"), null).get() == false);
    assertTrue(udf.evaluate(null, new Text("a,b,c")).get() == false);
    assertTrue(udf.evaluate(new Text("a,b,c"), new Text("")).get() == false);
    assertTrue(udf.evaluate(new Text(""), new Text("a,b,c")).get() == false);
    assertTrue(udf.evaluate(new Text(""), new Text("")).get() == false);
    assertTrue(udf.evaluate(new Text("a,b,c"), new Text("c,d,e")).get() == true);
    assertTrue(udf.evaluate(new Text("a,b,c"), new Text("d,e,f")).get() == false);
  }
}
