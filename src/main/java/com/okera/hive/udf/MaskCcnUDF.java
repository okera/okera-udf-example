package com.okera.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF which masks all characters but the last 4
 *
 * SELECT mask_ccn('1234-1234-1234-1234')
 * > 'XXXX-XXXX-XXXX-1234'
 */
@Description(name = "mask_ccn",
    value = "_FUNC_(x) - returns x, with the last 4 alphanumeric characters masked.")
public class MaskCcnUDF extends UDF {
  public Text evaluate(Text t) {
    if (t == null) return null;
    byte[] input = t.getBytes();
    byte[] result = new byte[t.getLength()];
    int i = 0;
    for (; i < t.getLength() - 4; ++i) {
      if (input[i] != '-') {
        result[i] = 'X';
      } else {
        result[i] = input[i];
      }
    }
    for (; i < t.getLength(); ++i) {
      result[i] = input[i];
    }
    return new Text(result);
  }
}
