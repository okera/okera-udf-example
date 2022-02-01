package com.okera.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF which masks all characters from the input string.
 *
 * SELECT mask('hello')
 * > 'xxxxx'
 */
@Description(name = "mask",
    value = "_FUNC_(x) - returns x with all characters maksed.")
public class MaskUDF extends UDF {
  public Text evaluate(Text t) {
    if (t == null) return null;
    byte[] result = new byte[t.getLength()];
    for (int i = 0; i < t.getLength(); i++) {
      result[i] = 'x';
    }
    return new Text(result);
  }
}
