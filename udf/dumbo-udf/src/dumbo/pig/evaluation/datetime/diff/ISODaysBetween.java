package dumbo.pig.evaluation.datetime.diff;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ISODaysBetween returns the number of days between two ISO8601 datetimes as a Long
 *
 * Jodatime: http://joda-time.sourceforge.net/
 * ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601
 *
 * Example usage:
 *
 * REGISTER /Users/me/commiter/piggybank/java/piggybank.jar ;
 * REGISTER /Users/me/commiter/piggybank/java/lib/joda-time-1.6.jar ;
 *
 * DEFINE ISOYearsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOYearsBetween();
 * DEFINE ISOMonthsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOMonthsBetween();
 * DEFINE ISODaysBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISODaysBetween();
 * DEFINE ISOHoursBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOHoursBetween();
 * DEFINE ISOMinutesBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOMinutesBetween();
 * DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();
 *
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (dt:chararray, dt2:chararray);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: chararray,dt2: chararray}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.000Z,2008-02-01T00:00:00.000Z)
 * (2008-02-06T02:06:02.000Z,2008-02-01T00:00:00.000Z)
 * (2007-03-05T03:05:03.000Z,2008-02-01T00:00:00.000Z)
 * ...
 *
 * diffs = FOREACH ISOin GENERATE ISOYearsBetween(dt, dt2) AS years,
 *  ISOMonthsBetween(dt, dt2) AS months,
 *  ISODaysBetween(dt, dt2) AS days,
 * 	ISOHoursBetween(dt, dt2) AS hours,
 * 	ISOMinutesBetween(dt, dt2) AS mins,
 * 	ISOSecondsBetween(dt, dt2) AS secs;
 *
 * DESCRIBE diffs;
 * diffs: {years: long,months: long,days: long,hours: long,mins: long,secs: long}
 *
 * DUMP diffs;
 *
 * (0L,11L,341,8185L,491107L,29466421L)
 * (0L,0L,5,122L,7326L,439562L)
 * (0L,-10L,-332,-7988L,-479334L,-28760097L)
 *
 */

public class ISODaysBetween extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException
    {
        if (input == null || input.size() < 2) {
            return null;
        }

        // Set the time to default or the output is in UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);

        DateTime startDate = new DateTime(input.get(0).toString());
        DateTime endDate = new DateTime(input.get(1).toString());

        // Larger date first
        Days d = Days.daysBetween(startDate, endDate);
        long days = d.getDays();

        return days;

    }

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
	}

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
    
  public static void main(String[] args) {
	  DateTime startDate = new DateTime("2009-02-07T03:07:01.000Z");
      DateTime endDate = new DateTime("2009-03-07T03:07:01.000Z");

      // Larger date first
      Days d = Days.daysBetween(startDate, endDate);
      long days = d.getDays();
      System.out.println(days);
    }
    
}