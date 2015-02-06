
/*
 * Copyright 2013 Thomas Cook Northern Europe AB
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package dumbo.pig.evaluation.datetime.truncate;

/**
 * ISOToGenericDateTime takes a datetime, input timezone and input format and transforms it into output timezone and output format.
 * See class SimpleDate for available date and time patterns http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
 *
 * Example usage:
 * 
 * REGISTER /dumbo-0.3.jar;
 *
 * DEFINE ISOToGeneric dumbo.pig.evaluation.datetime.truncate.ISOToGeneric();
 * 
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (dt:chararray, dt2:chararray);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: chararray,dt2: chararray}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.000Z)
 * (2008-02-06T02:06:02.000Z)
 * (2007-03-05T03:05:03.000Z)
 * ...
 *
 * truncated = FOREACH ISOin GENERATE 
 * 		ISOToGeneric(dt, "UTC", "Europe/Stockholm", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd") AS localDate,
 *     	ISOToGeneric(dt, "UTC", "Europe/Stockholm", "yyyy-MM-dd'T'HH:mm:ss", "HH:mm:ss") AS localTime;
 *
 * DESCRIBE truncated;
 * truncated: {localDate: chararray, localTime: chararray}
 *
 * DUMP truncated;
 * (2009-01-07,02:07:01)
 * (2008-02-06,03:06:02)
 * (2007-03-05,04:05:03)
 *
 */

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ISOToGenericDateTime extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }
        String isoDateString = input.get(0).toString();
        String inputTimezone = input.get(1).toString(); //UTC
        String outputTimezone = input.get(2).toString(); //"Europe/Stockholm"
        String inputFormat = input.get(3).toString(); //"yyyy-MM-dd'T'HH:mm:ss"
        String outputFormat = input.get(4).toString();//"yyyy-MM-dd"
        String result = getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, outputFormat);
        
        return result;
    }
    
    public static String getISOToGenericDateTime(String input, String inputTimeZone, String outputTimezone, String inputFormat, String outputFormat) throws IOException{
    	SimpleDateFormat sdf = new SimpleDateFormat(inputFormat);
    	sdf.setTimeZone(TimeZone.getTimeZone(inputTimeZone));
		Date dt;
		try {
			dt = sdf.parse(input);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			dt = null;
		}
		sdf = new SimpleDateFormat(outputFormat);
		sdf.setTimeZone(TimeZone.getTimeZone(outputTimezone));
	    return sdf.format(dt);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcList;
    }
    
    public static void main(String[] args) throws IOException {
    	String isoDateString = "2013-11-24 20:08:55.538";
    	
    	System.out.println("yyyy-MM-dd HH:mm:ss.SSS".substring(0,19));
    	System.out.println("Stockholm: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Stockholm", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" ));
    	/*
    	System.out.println("Stockholm: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Stockholm", "yyyy-MM-dd'T'HH:mm:sss'XXXX'Z", "yyyy-MM-dd HH:mm:ss" )); 
    	System.out.println("Helsinki: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Helsinki", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" ));
    	isoDateString = "2013-11-24T20:08:55.538";
    	System.out.println("Stockholm: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Stockholm", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" ));
    	System.out.println("Stockholm: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Stockholm", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" ));
    	System.out.println("Helsinki: " + getISOToGenericDateTime(isoDateString,"UTC", "Europe/Helsinki", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" ));
    	*/
    }
    
}

