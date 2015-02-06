

package dumbo.pig.util;

import org.apache.pig.EvalFunc;
//import org.apache.pig.FuncSpec;
//import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
//import org.apache.pig.impl.logicalLayer.FrontendException;
//import org.apache.pig.impl.logicalLayer.schema.Schema;


import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;



public class Querify extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException
    {
    	 if (input == null || input.size() < 3 || input.size() % 2 == 0){
             return null;
    	 }
    	 String output = "";
    	 try {
        	output = (String) input.get(0) + "?";
        	for (int i = 1; i < input.size(); i=i+2) {
        		String key = (String)input.get(i);
        		String val = (String) input.get(i+1);
        		if(val != null){
        			output = output + key + "=" + val;	
        			if(i+2 < input.size()){
            			output=output + "&";
            		}
        		}
        	}
    	 }catch(Exception e){}
        return output;
    }
/*
	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
	}

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
  */
    
  public static void main(String[] args) throws Exception {
	  Tuple t_new = TupleFactory.getInstance().newTuple();
      t_new.append("http://www.ving.se");
      t_new.append("key1");
      t_new.append(null);
      t_new.append("key2");
      t_new.append("value2");
      Querify q = new Querify();
      String qs = q.exec(t_new);
      System.out.println(qs);
    }
    
}