package com.okera.hive.udf;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

/**
 * UDF which sees whether two strings, representing comma-separated sets,
 * overlap.
 *
 * SELECT sets_intersect('a,b,c', 'd,e,c')
 * > true
 */
@Description(name = "sets_intersect",
    value = "_FUNC_(x, y) - returns whether x and y overlap as comma-separated sets.")
public class SetsIntersectUDF extends UDF {
  public BooleanWritable evaluate(Text text1, Text text2) {
    if (text1 == null || text2 == null) {
      return new BooleanWritable(false);
    }

    String str1 = text1.toString();
    String str2 = text2.toString();

    if (str1.length() == 0 || str2.length() == 0) {
      return new BooleanWritable(false);
    }

    HashSet<String> set1 = new HashSet<String>(Arrays.asList(str1.split(",")));
    HashSet<String> set2 = new HashSet<String>(Arrays.asList(str2.split(",")));

    set1.retainAll(set2);

    return new BooleanWritable(set1.size() > 0);
  }
}
