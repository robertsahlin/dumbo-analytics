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

package dumbo.pig.parsers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
//import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.commons.lang3.StringEscapeUtils;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class  URLQueryParserPrefix  extends EvalFunc <Map<String,String>> {
 
public Map<String,String> exec(Tuple input) throws IOException {  
	try{
		String url = (String) input.get(0);
		String prefix = (String) input.get(1);
		Map<String, String> output = getQueryMap(url,prefix);
		return output;
	}
	catch(Exception e) {}
	return null;
}

public static Map<String, String> getQueryMap(String url) throws MalformedURLException {
	return getQueryMap(url, "");
}

public static Map<String, String> getQueryMap(String url,String prefix) throws MalformedURLException {  
	
	Map<String, String> map = new HashMap<String, String>();
	String name, query;
	ArrayList<String> s = new ArrayList<String>(); 
	
	URL urla = new URL(url);
	map.put(prefix + "host",urla.getHost());
	map.put(prefix + "path",urla.getPath());
	map.put(prefix + "url", url);
	query = urla.getQuery();
	
    //Pattern re = Pattern.compile("[^&]+\\=\"[^\"]+\"|[^&]+\\=[^&]+");
	Pattern re = Pattern.compile("[^?\"]+\\?+|[^&]+\\=\"[^\"]+\"|[^&]+\\=[^&]+");
    
    query = StringEscapeUtils.unescapeHtml4(url);
    //System.out.println(query);
    Matcher m = re.matcher(query);
      while (m.find()){
        for( int groupIdx = 0; groupIdx < m.groupCount()+1; groupIdx++ ){
        	s.add(m.group(groupIdx));
        }
      }
      
	for (String param : s){
		if(param.indexOf("=") != -1){
			name = param.substring(0,param.indexOf("="));
			String decodedValue = "", encodedValue = "";
			try{
				encodedValue = param.substring(param.indexOf("=")+1);
				//encodedValue.replaceAll("%u00", "");
				//decodedValue = URLDecoder.decode(encodedValue, "UTF-8");
				//decodedValue = decodedValue.replace('"', '\0').trim();
				decodedValue = encodedValue.replace('"', '\0').trim();
			}
			catch(Exception e) {
				System.out.println(e.toString());
			}
			map.put(prefix + name, decodedValue);				
		}
	}
	
	return map;  
}

	public static void main(String[] args) throws MalformedURLException {
		String testUrl = "http://www.tjareborg.fi/pricecalendarlist?BookingQuery=-1%253a4542%253ad2013110112728%253a12505%253a8%253a5_ATSA+0%253a4_42%253a42%253a0%253a4%253a-1%253a0%253a2%253a&LmsTypeId=2&QueryDepID=12728&QueryCtryID=4542&QueryResID=6199&QueryDepDate=20131101&QueryDur=8&QueryAges=42%252c42%252c5%252c5&SelectedQueryUnits=1&SelectedDepDate=20131101&SelectedDepCd=HEL&SelectedDestCd=LPA&SelectedDur=8&QueryUnits=0&RoomCd=A34SER&SelectedClassCd=E&SelectedSerNo=51&SelectedHotCd=BALB&SelectedMUCD=TF"; 
		//"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&amp;referrer=&quot;http://www.tjareborg.fi/pricecalendarlist?BookingQuery=-1%253a4542%253ad2013110112728%253a12505%253a8%253a5_ATSA+0%253a4_42%253a42%253a0%253a4%253a-1%253a0%253a2%253a&amp;LmsTypeId=2&amp;QueryDepID=12728&amp;QueryCtryID=4542&amp;QueryResID=6199&amp;QueryDepDate=20131101&amp;QueryDur=8&amp;QueryAges=42%252c42%252c5%252c5&amp;SelectedQueryUnits=1&amp;SelectedDepDate=20131101&amp;SelectedDepCd=HEL&amp;SelectedDestCd=LPA&amp;SelectedDur=8&amp;QueryUnits=0&amp;RoomCd=A34SER&amp;SelectedClassCd=E&amp;SelectedSerNo=51&amp;SelectedHotCd=BALB&amp;SelectedMUCD=TF&quot;&amp;title=Tj&#228;reborg – Varaus&amp;screenSize=1301x615&amp;pageType=/Charter/CharterAccomodationInfo&amp;sessionId=hk4tbpn2hwpl0zosxjrmcdqn&amp;cb=9687234723954&amp;visitorId=C7CE1A67409279CD&amp;domain=www.tjareborg.fi&amp;page=/charteraccomodationinfo"; 
		//"http://tcnedev.blob.core.windows.net/collect/dumbo.gif?visitorId=51780014.1011678368.1370871877.1381928636.1382366200.46&amp;hitType=pageview&amp;referrer=&quot;http://vingse.dev.int/hotell?fuu=bar&lollo=bernie&quot;&amp;title=Nyheter och erbjudanden hos Ving&amp;screenSize=1920x595&amp;pageType=/static/StaticHtmlPage&amp;sessionId=h5m0rqvk4j0kd0neh0n5caox&amp;cb=4503631109837&amp;domain=vingse.dev.int&amp;page=/nyheter-erbjudanden";
		//"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&amp;referrer=&quot;http://www.ving.se/bokapaket?QueryDepID=2770&amp;QueryCtryID=-1&amp;QueryAreaID=-1&amp;QueryResID=-1&amp;QueryDestID=0&amp;QueryDepDate=20131225&amp;QueryChkInDate=00010101&amp;QueryDur=8&amp;QueryRetDate=20140102&amp;QueryChkOutDate=00010101&amp;CategoryId=2&amp;QueryRoomAges=%257c42%252c42%252c13&amp;QueryUnits=0&quot;&amp;title=Ving - Bokning&amp;screenSize=768x900&amp;pageType=/Charter/CharterAccomodationInfo&amp;sessionId=marqlh4dzwtnodsanbcstb0y&amp;cb=4602036443538&amp;visitorId=296AABDFAD3431DB&amp;domain=www.ving.se&amp;page=/charteraccomodationinfo"; 
		//"http://tcnedev.blob.core.windows.net/collect/dumbo.gif?visitorId=51780014.1011678368.1370871877.1381928636.1382366200.46&amp;hitType=pageview&amp;referrer=&quot;http://vingse.dev.int/hotell?fuu=bar&quot;&amp;title=Nyheter och erbjudanden hos Ving&amp;screenSize=1920x595&amp;pageType=/static/StaticHtmlPage&amp;sessionId=h5m0rqvk4j0kd0neh0n5caox&amp;cb=4503631109837&amp;domain=vingse.dev.int&amp;page=/nyheter-erbjudanden"; 
		//"http://tcneacctest.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&referrer=%22http%3A%2F%2Fvingse.acctest.int%2Fbokapaket%3FQueryDepID%3D2788%26QueryCtryID%3D65%26QueryAreaID%3D0%26QueryResID%3D279%26QueryDestID%3D0%26QueryDepDate%3D20131030%26QueryChkInDate%3D00010101%26QueryDur%3D8%26QueryRetDate%3D20131107%26QueryChkOutDate%3D00010101%26CategoryId%3D2%26QueryRoomAges%3D%257c42%26QueryUnits%3D0%22&title=Weekend%20med%20Ving%20-%20Weekendresor%20och%20f%C3%A4rdiga%20weekendpaket&screenSize=1920x542&pageType=%2FGeographical%2FCity&sessionId=f0dbi4xvlpo302o54n5qapvx&experiment=Bookingstartv2%3AOld&membershipId=1669299&customerId=3021233&cb=1509388845879&visitorId=cookieId%3Dc00f83c5-2121-e311-973f-001dd8b71d53&domain=vingse.acctest.int&page=%2Fweekend"; 
		//"http://tcneprod.blob.core.windows.net/collect/hadoop.gif?hitType=pageview&amp;referrer=http://www.tjareborg.fi/kanariansaaret/puerto-de-la-cruz/tenerife-ving/valmismatkapaketit?SelectedDepCd=TMP&amp;SelectedDepDate=20131029&amp;SelectedDestCd=TFS&amp;SelectedDur=8&amp;SelectedClassCd=E&amp;SelectedSerNo=21&amp;SelectedMUCD=TF&amp;SelectedHotCd=TENV&amp;SelectedHotelDestCd=TFS&amp;RoomCd=A12&amp;QueryDepCD=TMP&amp;QueryDepID=12736&amp;QueryDepDate=20131029&amp;QueryUnits=0&amp;QueryAges=42%252c42&amp;QueryDur=8&amp;utm_content=2589&amp;v_70=HH&amp;v_71=vk_42&amp;title=Hotel Botanico, Puerto de la Cruz – Varaa matka Tj&#228;reborgilta&amp;screenSize=1600x728&amp;pageType=/Geographical/HotelInfo&amp;sessionId=d5jngren5r3edglnqug5zaia&amp;cb=2366503950730&amp;visitorId=817CF633B5A4152C&amp;domain=www.tjareborg.fi&amp;page=/kanariansaaret/puerto-de-la-cruz/hotel-botanico"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;referrer=http://vingse.acctest.int/weekend&amp;title=Regulj&#228;rbokning - V&#228;lj hotell&amp;screenSize=1920x640&amp;pageType=/Independent/PackageIndependentAccomodationList&amp;sessionId=u4qxq1glya2aaeou03x30tvd&amp;experiment=Bookingstartv2:Old&amp;membershipId=1669299&amp;customerId=3021233&amp;cb=4476702562533&amp;visitorId=cookieId=c00f83c5-2121-e311-973f-001dd8b71d53&amp;domain=vingse.acctest.int&amp;page=/packageindependentacclist";
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;href=%22http%3A%2F%2Fvingse.acctest.int%2Fbokapaket%3FQueryDepID%3D2788%26QueryCtryID%3D-1%26QueryAreaID%3D-1%26QueryResID%3D-1%26QueryDestID%3D0%26QueryDepDate%3D20140425%26QueryChkInDate%3D00010101%26QueryDur%3D8%26QueryRetDate%3D20140503%26QueryChkOutDate%3D00010101%26CategoryId%3D2%26QueryRoomAges%3D%257c42%26QueryUnits%3D1%22&amp;travelAction=Search&amp;travelType=CharterPackage&amp;travelDepartureCode=ARN&amp;travelDuration=8&amp;travelDate=2014-04-25&amp;travelHits=8&amp;travelSearchPage=%2FCharter%2FCharterPackageList&amp;travelPax=1&amp;cb=3545790857169&amp;visitorId=cookieId%3Dc00f83c5-2121-e311-973f-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?&hitType=event";
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;href=%22http%3A%2F%2Fvingse.acctest.int%2Fbokapaket%3FQueryDepID%3D2788%26QueryCtryID%3D-1%26QueryAreaID%3D-1%26QueryResID%3D-1%26QueryDestID%3D0%26QueryDepDate%3D20140425%26QueryChkInDate%3D00010101%26QueryDur%3D8%26QueryRetDate%3D20140503%26QueryChkOutDate%3D00010101%26CategoryId%3D2%26QueryRoomAges%3D%257c42%26QueryUnits%3D1%22&amp;travelAction=Search&amp;travelType=CharterPackage&amp;travelDepartureCode=ARN&amp;travelDuration=8&amp;travelDate=2014-04-25&amp;travelHits=8&amp;travelSearchPage=%2FCharter%2FCharterPackageList&amp;travelPax=1&amp;cb=3545790857169&amp;visitorId=cookieId%3Dc00f83c5-2121-e311-973f-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;href=&quot;http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;QueryCtryID=-1&amp;QueryAreaID=-1&amp;QueryResID=-1&amp;QueryDestID=0&amp;QueryDepDate=20140425&amp;QueryChkInDate=00010101&amp;QueryDur=8&amp;QueryRetDate=20140503&amp;QueryChkOutDate=00010101&amp;CategoryId=2&amp;QueryRoomAges=%257c42&amp;QueryUnits=1&quot;&amp;travelAction=Search&amp;travelType=CharterPackage&amp;travelDepartureCode=ARN&amp;travelDuration=8&amp;travelDepartureDate=2014-04-25&amp;travelHits=8&amp;travelSearchPage=/Charter/CharterPackageList&amp;travelPax=1&amp;cb=4431083227973&amp;visitorId=cookieId=c00f83c5-2121-e311-973f-001dd8b71d53&amp;domain=vingse.acctest.int&amp;page=/bokapaket"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;href=%22http%3A%2F%2Fvingse.acctest.int%2Fbokapaket%3FQueryDepID%3D2788%26QueryCtryID%3D-1%26QueryAreaID%3D-1%26QueryResID%3D-1%26QueryDestID%3D0%26QueryDepDate%3D20140425%26QueryChkInDate%3D00010101%26QueryDur%3D8%26QueryRetDate%3D20140503%26QueryChkOutDate%3D00010101%26CategoryId%3D2%26QueryRoomAges%3D%257c42%26QueryUnits%3D1%22&amp;travelAction=Search&amp;travelType=CharterPackage&amp;travelDepartureCode=ARN&amp;travelDuration=8&amp;travelDate=2014-04-25&amp;travelHits=8&amp;travelSearchPage=%2FCharter%2FCharterPackageList&amp;travelPax=1&amp;cb=3545790857169&amp;visitorId=cookieId%3Dc00f83c5-2121-e311-973f-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;QueryCtryID=65&amp;QueryAreaID=110&amp;QueryResID=0&amp;QueryDestID=0&amp;QueryDepDate=20140510&amp;QueryChkInDate=00010101&amp;QueryDur=8&amp;QueryRetDate=20140518&amp;QueryChkOutDate=00010101&amp;CategoryId=2&amp;QueryRoomAges=%257c42&amp;QueryUnits=0&amp;referrer=http://vingse.acctest.int/&amp;title=Boka din resa hos Ving&amp;screenSize=1920x1067&amp;pageType=/Charter/CharterPackageList&amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;membershipId=5608895&amp;customerId=5944999&amp;cb=5010788429062&amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53";
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;href=http://vingse.acctest.int/&amp;referrer=&amp;title=Resor med charter och regulj&#228;rflyg - Boka din resa hos Ving&amp;screenSize=1920x1067&amp;pageType=/Start&amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;membershipId=5608895&amp;customerId=5944999&amp;cb=2990338248200&amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;eventCategory=travel&amp;eventAction=Search&amp;eventLabel=CharterPackage&amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42%252c42%252c42%252c42&amp;amp;QueryUnits=0&amp;amp;travelAction=Search&amp;amp;travelType=CharterPackage&amp;amp;travelDepartureCode=ARN&amp;amp;travelDestinationCode=IBZ&amp;amp;travelDuration=8&amp;amp;travelDate=2014-05-10&amp;amp;travelHits=7&amp;amp;travelSearchPage=/Charter/CharterPackageList&amp;amp;travelPax=4&amp;amp;cb=6388402390293&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;amp;eventCategory=travel&amp;amp;eventAction=Search&amp;amp;eventLabel=CharterPackage&amp;amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;travelAction=Search&amp;amp;travelType=CharterPackage&amp;amp;travelDepartureCode=ARN&amp;amp;travelDestinationCode=IBZ&amp;amp;travelDuration=8&amp;amp;travelDate=2014-05-10&amp;amp;travelHits=7&amp;amp;travelSearchPage=/Charter/CharterPackageList&amp;amp;travelPax=1&amp;amp;cb=6740135459695&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;amp;href=http://vingse.acctest.int/charteraccomodationinfo?ItemId=67822&amp;amp;SelectedPackage=57_cn3_IBZ4_ORQU2_VS1%253a3_A12c1_E3_ARNd201405103_IBZ8%253a2_VS2_611%253a&amp;amp;BookingQuery=110%253a65%253ad201405102788%253a0%253a8%253an0%253a1_42%253a0%253a0%253a2%253a&amp;amp;UseBookingFlow=True&amp;amp;referrer=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;title=Ving - Bokning&amp;amp;screenSize=1920x1067&amp;amp;pageType=/Charter/CharterAccomodationInfo&amp;amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;amp;membershipId=5608895&amp;amp;customerId=5944999&amp;amp;cb=9205928801093&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;amp;eventCategory=travel&amp;amp;eventAction=Search&amp;amp;eventLabel=CharterPackage&amp;amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;travelAction=Search&amp;amp;travelType=CharterPackage&amp;amp;travelDepartureCode=ARN&amp;amp;travelDestinationCode=IBZ&amp;amp;travelDuration=8&amp;amp;travelDate=2014-05-10&amp;amp;travelHits=7&amp;amp;travelSearchPage=/Charter/CharterPackageList&amp;amp;travelPax=1&amp;amp;cb=5385939574334&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42%252c42%252c42%252c42&amp;amp;QueryUnits=0&amp;amp;referrer=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;title=Boka din resa hos Ving&amp;amp;screenSize=1920x1067&amp;amp;pageType=/Charter/CharterPackageList&amp;amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;amp;membershipId=5608895&amp;amp;customerId=5944999&amp;amp;cb=1525626939255&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;referrer=http://vingse.acctest.int/&amp;amp;title=Boka din resa hos Ving&amp;amp;screenSize=1920x1067&amp;amp;pageType=/Charter/CharterPackageList&amp;amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;amp;membershipId=5608895&amp;amp;customerId=5944999&amp;amp;cb=6128791600931&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=event&amp;amp;eventCategory=travel&amp;amp;eventAction=Search&amp;amp;eventLabel=CharterPackage&amp;amp;href=http://vingse.acctest.int/charteraccomodationinfo?ItemId=67822&amp;amp;SelectedPackage=57_cn3_IBZ4_ORQU2_VS1%253a3_A12c1_E3_ARNd201405103_IBZ8%253a2_VS2_611%253a&amp;amp;BookingQuery=110%253a65%253ad201405102788%253a0%253a8%253an0%253a1_42%253a0%253a0%253a2%253a&amp;amp;UseBookingFlow=True&amp;amp;travelAction=Search&amp;amp;travelType=CharterPackage&amp;amp;travelDepartureCode=ARN&amp;amp;travelDestinationCode=IBZ&amp;amp;travelHotelCode=IBZORQU&amp;amp;travelDuration=8&amp;amp;travelDate=2014-05-10&amp;amp;travelHits=8&amp;amp;travelSearchPage=/Charter/CharterAccomodationInfo&amp;amp;travelPax=1&amp;amp;cb=9438755230512&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53";
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;amp;href=http://vingse.acctest.int/&amp;amp;referrer=&amp;amp;title=Resor med charter och regulj&amp;#228;rflyg - Boka din resa hos Ving&amp;amp;screenSize=1920x1067&amp;amp;pageType=/Start&amp;amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;amp;membershipId=5608895&amp;amp;customerId=5944999&amp;amp;cb=2990338248200&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53)";
		//http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;amp;href=http://vingse.acctest.int/bokapaket?QueryDepID=2788&amp;amp;QueryCtryID=65&amp;amp;QueryAreaID=110&amp;amp;QueryResID=0&amp;amp;QueryDestID=0&amp;amp;QueryDepDate=20140510&amp;amp;QueryChkInDate=00010101&amp;amp;QueryDur=8&amp;amp;QueryRetDate=20140518&amp;amp;QueryChkOutDate=00010101&amp;amp;CategoryId=2&amp;amp;QueryRoomAges=%257c42&amp;amp;QueryUnits=0&amp;amp;referrer=http://vingse.acctest.int/&amp;amp;title=Boka din resa hos Ving&amp;amp;screenSize=1920x1067&amp;amp;pageType=/Charter/CharterPackageList&amp;amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;amp;membershipId=5608895&amp;amp;customerId=5944999&amp;amp;cb=5010788429062&amp;amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?foo=bar&=";
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&href=http%3A%2F%2Fvingse.acctest.int%2Fbokapaket%3FQueryDepID%3D2788%26QueryCtryID%3D-1%26QueryAreaID%3D-1%26QueryResID%3D-1%26QueryDestID%3D0%26QueryDepDate%3D20140425%26QueryChkInDate%3D00010101%26QueryDur%3D8%26QueryRetDate%3D20140503%26QueryChkOutDate%3D00010101%26CategoryId%3D2%26QueryRoomAges%3D%257c42%26QueryUnits%3D1&domain=vingse.acctest.int&page=%2Fbokapaket&referrer=http%3A%2F%2Fvingse.acctest.int%2F&title=Boka%20din%20resa%20hos%20Ving&screenSize=1920x760&pageType=%2FCharter%2FCharterPackageList&sessionId=zgxitbmfmkwr4e3hjfj0wkad&experiment=Bookingstartv2%3AOld&membershipId=1669299&customerId=3021233&cb=2433429788798&visitorId=cookieId%3Dc00f83c5-2121-e311-973f-001dd8b71d53"; 
		//"http://tcnehadoop.blob.core.windows.net/tracker/hadoop.gif?hitType=pageview&amp;href=http://vingse.acctest.int/&amp;referrer=&amp;title=Resor med charter och regulj&#228;rflyg - Boka din resa hos Ving&amp;screenSize=1920x1067&amp;pageType=/Start&amp;sessionId=tjt513nxpxhi3uaupjxuuz1z&amp;membershipId=5608895&amp;customerId=5944999&amp;cb=2990338248200&amp;visitorId=cookieId=8423d5ae-cc1e-e311-9e76-001dd8b71d53"; 
		//"&page_type=%2fGeographical%2fHotelInfo&dt=Green+Beach+-+Hotell+i+Arguinegu%u00edn+%7c+Ving";

		Map<String, String> map = getQueryMap(testUrl,"bla_");  
		Set<String> keys = map.keySet();  
		for (String key : keys){  
			System.out.println(key + " = " + map.get(key));
		}
	}

}

