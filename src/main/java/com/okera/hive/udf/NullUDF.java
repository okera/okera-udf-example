package com.okera.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
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

/**
 * UDF which returns null for all values
 *
 * SELECT null('hello')
 * > NULL
 */
@Description(name = "null",
    value = "_FUNC_(x) - returns null.")
public class NullUDF extends UDF {
  public BooleanWritable evaluate(BooleanWritable t) {
    return null;
  }

  public ByteWritable evaluate(ByteWritable t) {
    return null;
  }

  public ShortWritable evaluate(ShortWritable t) {
    return null;
  }

  public IntWritable evaluate(IntWritable t) {
    return null;
  }

  public LongWritable evaluate(LongWritable t) {
    return null;
  }

  public FloatWritable evaluate(FloatWritable t) {
    return null;
  }

  public DoubleWritable evaluate(DoubleWritable t) {
    return null;
  }

  public Text evaluate(Text t) {
    return null;
  }

  public DateWritable evaluate(DateWritable t) {
    return null;
  }

  public TimestampWritable evaluate(TimestampWritable t) {
    return null;
  }

  public HiveDecimalWritable evaluate(HiveDecimalWritable t) {
    return null;
  }
}
