
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
 * 
 */

package dumbo.pig.evaluation.datetime.truncate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class  ISOToDateTimeMap  extends EvalFunc <Map<String,String>> {
	 
public Map<String,String> exec(Tuple input) throws IOException {  
	try{
		  String isoDateString = input.get(0).toString();
	        String inputTimezone = input.get(1).toString();
	        String outputTimezone = input.get(2).toString();
	        String inputFormat = input.get(3).toString(); 
		Map<String, String> output = getISOToDateTimeMap(isoDateString, inputTimezone, outputTimezone, inputFormat);
		return output;
	}
	catch(Exception e) {}
	return null;
}

public static Map<String, String> getISOToDateTimeMap(String isoDateString, String inputTimezone, String outputTimezone, String inputFormat) throws IOException{  
	
	Map<String, String> map = new HashMap<String, String>();
	map.put("date", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "yyyy-MM-dd"));
	map.put("time", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "HH:mm:ss"));
	map.put("year", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "yyyy"));
	map.put("monthOfYear", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "MM"));
	map.put("yearWeek", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "yyyy-w"));
	map.put("weekOfYear", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "w"));
	map.put("dayOfMonth", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "dd"));
	map.put("dayOfWeek", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "EEEE"));
	map.put("dayOfYear", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "D"));
	map.put("hourOfDay", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "HH"));
	map.put("minuteOfHour", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "mm"));
	map.put("dateTime", ISOToGenericDateTime.getISOToGenericDateTime(isoDateString, inputTimezone, outputTimezone, inputFormat, "yyyy-MM-dd HH:mm:ss"));
	return map;
}

public static void main(String[] args) throws IOException{
	
	String isoDateString = "2009-02-07T03:07:01.000Z";
	String inputTimezone = "UTC";
	String outputTimezone = "Europe/Stockholm";
	String inputFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	Map<String, String> map = getISOToDateTimeMap(isoDateString, inputTimezone, outputTimezone, inputFormat);  
	Set<String> keys = map.keySet();  
	for (String key : keys){  
		System.out.println(key + " = " + map.get(key));
	}
}


}