package com.okera.hive.udf;

import java.sql.Date;
import java.util.Calendar;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;

@Description(name = "phi_dob",
    value = "_FUNC_(x) - returns the date anonymized per PHI rules.")
public class PhiDobUDF extends UDF {

  public DateWritable evaluate(DateWritable date) {
    if (date == null) return null;
    final int currentYear = Calendar.getInstance().get(Calendar.YEAR);

    Calendar cal = Calendar.getInstance();
    cal.setTime(date.get());
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.DATE, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    if (currentYear - cal.get(Calendar.YEAR) >= 90) {
      cal.set(Calendar.YEAR, currentYear - 90);
      return new DateWritable(new Date(cal.getTime().getTime()));
    }
    return new DateWritable(new Date(cal.getTime().getTime()));
  }
}
