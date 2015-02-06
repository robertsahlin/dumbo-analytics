package dumbo.pig.maps;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class  MergeMaps  extends EvalFunc <Map<String,String>> {
	 
@SuppressWarnings("unchecked")
public Map<String,String> exec(Tuple input) throws IOException {  
	try{
		Map<String, String> output = new HashMap<String, String>();
		output.put("key", "value");
		for(int i=0; i< input.size();i++){
			if(input.get(i).getClass().equals(output.getClass())){
				
				Map<String, String> inputMap = (HashMap<String,String>) input.get(i);
				output.putAll(inputMap);
				
			}
		}
		return output;
	}
	catch(Exception e) {}
	return null;
}

public static void main(String[] args) throws IOException{
	Map<String, String> map1 = new HashMap<String, String>();
	Map<String, String> map2 = new HashMap<String, String>();
	map1.put("date", "idag");
	map1.put("time", "idag");
	map2.put("time", "imorn");
	if(map2.getClass() == map1.getClass()){
		System.out.println(map2.getClass());
	}
	map1.putAll(map2);
	Set<String> keys = map1.keySet();  
	for (String key : keys){  
		System.out.println(key + " = " + map1.get(key));
	}
}


}
