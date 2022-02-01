package com.okera.hive.udf;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "phi_zip",
    value = "_FUNC_(x) - returns the zipcode anonymized per PHI rules.")
public class PhiZip3UDF extends UDF {
  private static final Text ZEROES = new Text("000");
  private static final Set<Text> ZIP3_EXCLUSIONS = new HashSet<>();

  static {
    // Based on 2010 census
    ZIP3_EXCLUSIONS.add(new Text("036"));
    ZIP3_EXCLUSIONS.add(new Text("059"));
    ZIP3_EXCLUSIONS.add(new Text("102"));
    ZIP3_EXCLUSIONS.add(new Text("202"));
    ZIP3_EXCLUSIONS.add(new Text("203"));
    ZIP3_EXCLUSIONS.add(new Text("204"));
    ZIP3_EXCLUSIONS.add(new Text("205"));
    ZIP3_EXCLUSIONS.add(new Text("369"));
    ZIP3_EXCLUSIONS.add(new Text("556"));
    ZIP3_EXCLUSIONS.add(new Text("692"));
    ZIP3_EXCLUSIONS.add(new Text("753"));
    ZIP3_EXCLUSIONS.add(new Text("772"));
    ZIP3_EXCLUSIONS.add(new Text("821"));
    ZIP3_EXCLUSIONS.add(new Text("823"));
    ZIP3_EXCLUSIONS.add(new Text("878"));
    ZIP3_EXCLUSIONS.add(new Text("879"));
    ZIP3_EXCLUSIONS.add(new Text("884"));
    ZIP3_EXCLUSIONS.add(new Text("893"));
  }

  public Text evaluate(Text t) {
    if (t == null) return null;
    // if zip.len > 5, return the same assuming that it is not a zip code
    if (t.getLength() > 5 || t.getLength() == 0) return t;

    byte[] input = t.getBytes();
    byte[] zip3 = new byte[3];
    zip3[0] = zip3[1] = zip3[2] = '0';

    int i = 0;
    if (input.length < 3) {
      i = 3 - input.length;
    }

    for (int j = 0; i < 3; ++i, ++j) {
      zip3[i] = input[j];
    }

    final Text result = new Text(zip3);
    // Check if zip3 is a part of exclusion list or not
    // if yes, return "000" else return zip3
    if (ZIP3_EXCLUSIONS.contains(result)) {
      return ZEROES;
    } else {
      return result;
    }
  }
}
