package com.okera.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

@Description(name = "phi_age",
    value = "_FUNC_(x) - returns the age anonymized per PHI rules.")
public class PhiAgeUDF extends UDF {
  private static final IntWritable CLAMPED = new IntWritable(90);

  public IntWritable evaluate(IntWritable t) {
    if (t == null) return null;
    if (t.get() >= 90) return CLAMPED;
    return t;
  }
}
