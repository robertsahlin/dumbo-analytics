package dumbo.pig.maps;

import java.io.IOException;
//import java.util.HashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
//import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;



public class AppendToMap extends EvalFunc <Map<String,Object>> {
	
	    @SuppressWarnings("unchecked")
		@Override
	    public Map<String,Object> exec(Tuple input) throws IOException {
	        if (input == null || input.size() < 3 || input.size() % 2 == 0)
	                return null;
	        try {
	           	Map<String, Object> output = new HashMap<String, Object>();
	            output = (HashMap<String,Object>) input.get(0);
	            //Map<String, Object> output = new TreeMap<String, Object>();
	            //output = (TreeMap<String,Object>) input.get(0);
	
	            for (int i = 1; i < input.size(); i=i+2) {
	                String key = (String)input.get(i);
	                Object val = input.get(i+1);    
	                output.put(key, val);
	            }
	
	            return output;
	        } catch (ClassCastException e){
	                throw new RuntimeException("Map key must be a String");
	        } catch (ArrayIndexOutOfBoundsException e){
	                throw new RuntimeException("Function input must have odd number of parameters");
	        } catch (Exception e) {
	            throw new RuntimeException("Error while creating a map", e);
	        }
	    }
	
	    @Override
	    public Schema outputSchema(Schema input) {
	        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
	    }
	
	    public static void main(String[] args) throws IOException{
	    	Map<String, String> map1 = new HashMap<String, String>();
	    	//Map<String, Object> map1 = new TreeMap<String, Object>();
	    	Map<String, Object> map2 = new HashMap<String, Object>();
	    	//Map<String, Object> map2 = new TreeMap<String, Object>();
	    	//map1.put("date", "idag");
	    	map1.put("key1", "value1");
	    	Tuple t_new = TupleFactory.getInstance().newTuple();
	        t_new.append(map1);
	        t_new.append("key2");
	        t_new.append("value2");
	        t_new.append("key3");
	        t_new.append("value3");
	        AppendToMap atm = new AppendToMap();
	        map2 = atm.exec(t_new);
	        
	    	Set<String> keys = map2.keySet();  
	    	for (String key : keys){  
	    		System.out.println(key + " = " + map2.get(key));
	    	}
	    }
	}
