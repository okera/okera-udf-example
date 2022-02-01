package com.okera.hive.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
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
 * UDF which returns zero for all values
 *
 * SELECT zero('hello')
 * > ''
 */
@Description(name = "zero",
    value = "_FUNC_(x) - returns zero or empty for the data type for x.")
public class ZeroUDF extends UDF {
  private static final Text EMPTY = new Text("");
  private static final BooleanWritable FALSE = new BooleanWritable(false);
  private static final ByteWritable ZERO_BYTE = new ByteWritable((byte)0);
  private static final ShortWritable ZERO_SHORT = new ShortWritable((short)0);
  private static final IntWritable ZERO_INT = new IntWritable(0);
  private static final LongWritable ZERO_LONG = new LongWritable(0);
  private static final FloatWritable ZERO_FLOAT = new FloatWritable(0);
  private static final DoubleWritable ZERO_DOUBLE = new DoubleWritable(0);
  private static final DateWritable ZERO_DATE = new DateWritable(0);

  // This constructor changed across hive versions. Be careful changing this. This
  // is picked to be version compatible.
  private static final HiveDecimalWritable ZERO_DECIMAL =
      new HiveDecimalWritable(HiveDecimal.create(0));
  private static final TimestampWritable ZERO_TIMESTAMP = new TimestampWritable();

  public BooleanWritable evaluate(BooleanWritable t) {
    return FALSE;
  }

  public ByteWritable evaluate(ByteWritable t) {
    return ZERO_BYTE;
  }

  public ShortWritable evaluate(ShortWritable t) {
    return ZERO_SHORT;
  }

  public IntWritable evaluate(IntWritable t) {
    return ZERO_INT;
  }

  public LongWritable evaluate(LongWritable t) {
    return ZERO_LONG;
  }

  public FloatWritable evaluate(FloatWritable t) {
    return ZERO_FLOAT;
  }

  public DoubleWritable evaluate(DoubleWritable t) {
    return ZERO_DOUBLE;
  }

  public Text evaluate(Text t) {
    return EMPTY;
  }

  public DateWritable evaluate(DateWritable t) {
    return ZERO_DATE;
  }

  public TimestampWritable evaluate(TimestampWritable t) {
    return ZERO_TIMESTAMP;
  }

  public HiveDecimalWritable evaluate(HiveDecimalWritable t) {
    return ZERO_DECIMAL;
  }
}
