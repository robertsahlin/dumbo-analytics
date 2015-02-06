
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

package dumbo.pig.util;

//remember to compile in Java version 1.5 since 1.6 or 1.7 doesn't work

import java.io.IOException;
//import java.net.URLDecoder;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.commons.lang3.StringEscapeUtils;

public class UnescapeHtml extends EvalFunc <String> {

public String exec(Tuple input) throws IOException {  
	try{
		String line = (String) input.get(0);
		String output = getUnescapedHtml(line);
		return output;
	}
	catch(Exception e) {}
	return null;
}
  
public static String getUnescapedHtml(String line) {  
	
	String output = "";
	if(line.length()>0){
		try{
			output = StringEscapeUtils.unescapeHtml4(line);
			//output = URLDecoder.decode(output, "UTF-8");
		}
		catch(Exception e) {System.out.println(e.toString());}				
	}
	return output;  
}

public static void main(String[] args) {

	String[] testUrl = {
			"1.0;2013-10-17T08:59:23.1386269Z;GetBlob;AnonymousSuccess;200;4;4;anonymous;;tcneprod;blob;\"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&amp;referrer=http://www3.ving.no/VingNO/HappyHour/2013/Happy-hour-2013-42.html&amp;title=Apartamentos Aguamarina - Hotell i San Miguel de Abona | Ving&amp;screenSize=1920x931&amp;pageType=/Geographical/HotelInfo_PackagePriceMatrix&amp;sessionId=hjz2sjrawedeipboahs1gsyv&amp;cb=9704932868109&amp;visitorId=9B2CC9D1C530526C&amp;domain=www.ving.no&amp;page=/kanarioyene/san-miguel-de-abona/apartamentos-aguamarina/pakkepris\";\"/tcneprod/collect/hadoop.gif\";916e5e95-2748-4116-82d2-356802a15667;0;80.89.47.150:49289;2009-09-19;1089;0;302;43;0;;;\"0x8D0918345A2E842\";Monday, 07-Oct-13 14:47:25 GMT;;\"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)\";\"http://www.ving.no/kanarioyene/san-miguel-de-abona/apartamentos-aguamarina/pakkepris?SelectedDepCd=OSL&amp;SelectedDepDate=20131124&amp;SelectedDestCd=TFS&amp;SelectedDur=15&amp;SelectedClassCd=E&amp;SelectedSerNo=71&amp;SelectedMUCD=VN&amp;SelectedHotCd=AGUM&amp;SelectedHotelDestCd=TFS&amp;RoomCd=A22&amp;QueryDepCD=OSL&amp;QueryDepID=12667&amp;QueryDepDate=20131124&amp;QueryUnits=0&amp;QueryAges=42%2c42&amp;QueryDur=15&amp;utm_medium=newsletter\";",
			"1.0;2013-10-17T08:59:43.4556895Z;GetBlob;AnonymousSuccess;200;5;5;anonymous;;tcneprod;blob;\"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=event&amp;customerId=1222960&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;membershipId=2615105&amp;sessionId=kzjgffngnxykwg2xbssfdjv3&amp;travelAction=Search&amp;travelAdults=2&amp;travelAges=42,42&amp;travelChildren=0&amp;travelDepartureCode=OSL&amp;travelDepartureDate=2013-11-24&amp;travelDestinationCode=TFS&amp;travelDuration=15&amp;travelHits=15&amp;travelHotelCode=TFSAGUM&amp;travelPax=2&amp;travelResortCode=SMG&amp;travelSameRoom=false&amp;travelType=CharterPackage&amp;travelSearchPage=/Geographical/HotelInfo_PackagePriceMatrix&amp;cb=1526712793856&amp;visitorId=E64039667F7293F0&amp;domain=www.ving.no&amp;page=/kanarioyene/san-miguel-de-abona/apartamentos-aguamarina/pakkepris\";\"/tcneprod/collect/hadoop.gif\";c8b5d530-1f7b-4e47-8395-abf43473826b;0;80.212.87.168:53119;2009-09-19;1336;0;302;43;0;;;\"0x8D0918345A2E842\";Monday, 07-Oct-13 14:47:25 GMT;;\"Mozilla/5.0 (iPad; CPU OS 7_0_2 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11A501 Safari/9537.53\";\"http://www.ving.no/kanarioyene/san-miguel-de-abona/apartamentos-aguamarina/pakkepris?SelectedDepCd=OSL&amp;SelectedDepDate=20131124&amp;SelectedDestCd=TFS&amp;SelectedDur=15&amp;SelectedClassCd=E&amp;SelectedSerNo=71&amp;SelectedMUCD=VN&amp;SelectedHotCd=AGUM&amp;SelectedHotelDestCd=TFS&amp;RoomCd=A22&amp;QueryDepCD=OSL&amp;QueryDepID=12667&amp;QueryDepDate=20131124&amp;QueryUnits=0&amp;QueryAges=42%2c42&amp;QueryDur=15&amp;utm_medium=newsletter\";",
			"1.0;2013-10-17T08:59:23.1386269Z;GetBlob;AnonymousSuccess;200;4;4;anonymous;;tcneprod;blob;",
			"1.0;2013-10-17T08:59:23.1386269Z;GetBlob;AnonymousSuccess;200;4;4;anonymous;;tcneprod;blob;\"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&amp;referrer=http://www3.ving.no/VingNO/HappyHour/2013/Happy-hour-2013-42.html&amp;title=Apartamentos Aguamarina - Hotell i San Miguel de Abona | Ving&amp;screenSize=1920x931&amp;pageType=/Geographical/HotelInfo_PackagePriceMatrix&amp;sessionId=hjz2sjrawedeipboahs1gsyv&amp;cb=9704932868109&amp;visitorId=9B2CC9D1C530526C&amp;domain=www.ving.no&amp;page=/kanarioyene/san-miguel-de-abona/apartamentos-aguamarina/pakkepris\";\"/tcneprod/collect/hadoop.gif\";916e5e95-2748-4116-82d2-356802a15667;0;80.89.47.150:49289;2009-09-19;1089;0;302;43;0;;;\"0x8D0918345A2E842\";Monday, 07-Oct-13 14:47:25 GMT;",
			"Monday, 07-Oct-13 14:47:25 GMT;\"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)\";"
			};
	for(int i=0; i < testUrl.length; i++){
		String map = getUnescapedHtml(testUrl[i]);  
		System.out.println(map);
	}
}
}

