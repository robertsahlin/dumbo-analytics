package dumbo.pig.util;

import java.io.BufferedReader;
//import java.io.DataInputStream;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
//import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//import java.util.List;

//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
//import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.data.TupleFactory;

//java/com/acme/marketing/MetroResolverV2.java
/**
* A lookup UDF that maps cities to metropolatin areas, this time using the
* Distributed Cache.
*/

public class LookingAtLookup extends EvalFunc<String> {

 String lookupFile;
 HashMap<String, String> lookup = null;

 /*
  * @param file - File that contains a lookup table mapping cities to metro
  * areas.  The file must be located on the filesystem where this UDF will
  * run.
  */
 public LookingAtLookup(String file) {
     // Just store the filename. Don't load the lookup table, since we may
     // be on the frontend or the backend.
     lookupFile = file;
 }
 
 public String exec(Tuple input) throws IOException {
	    if (lookup == null) {
	        // We have not been initialized yet, so do it now.

	        lookup = new HashMap<String, String>();
	        URL oracle = new URL(lookupFile);
	        BufferedReader in = new BufferedReader(
	        new InputStreamReader(oracle.openStream()));

	        String inputLine;
	        while ((inputLine = in.readLine()) != null){
	        	String[] toks = new String[2];
            	toks = inputLine.split(":", 2);
            	lookup.put(toks[0], toks[1]);
	        }
	        in.close();
	        
	    }
	    return lookup.get((String)input.get(0));
	}

 public static void main(String[] args) throws IOException{
		String lookupfile = "http://tcneprod.blob.core.windows.net/lookup/cityResolver.csv";
		LookingAtLookup lal = new LookingAtLookup(lookupfile);
		Tuple t_new = TupleFactory.getInstance().newTuple();
        t_new.append("Helsinki");
        System.out.println(lal.exec(t_new));
	}
 
}