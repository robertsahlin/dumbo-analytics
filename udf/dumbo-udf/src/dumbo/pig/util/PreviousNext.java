package dumbo.pig.util;

import java.io.IOException;
import java.util.Iterator;


import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;



/**
 * Enumerate a bag, appending to each tuple its index within the bag.
 * 
 * <p>
 * For example:
 * <pre>
 *   {(A),(B),(C),(D)} => {(A,0),(B,1),(C,2),(D,3)}
 * </pre>
 * The first constructor parameter (optional) dictates the starting index of the counting.
 * This UDF implements the accumulator interface, reducing DataBag materialization costs.
 * </p>
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * define Enumerate datafu.pig.bags.Enumerate('1');
 *
 * -- input:
 * -- ({(100),(200),(300),(400)})
 * input = LOAD 'input' as (B: bag{T: tuple(v2:INT)});
 *
 * -- output:
 * -- ({(100,1),(200,2),(300,3),(400,4)})
 * output = FOREACH input GENERATE Enumerate(B);
 * }
 * </pre>
 */

public class PreviousNext extends AccumulatorEvalFunc<DataBag>
{
  private final int start;
  
  private DataBag outputBag;
  private long i;
  private long count;
  private String previous;
  private String next;

  public PreviousNext()
  {
    this("0");
  }

public PreviousNext(String start)
  {
    this.start = Integer.parseInt(start);
    this.previous = "previous";
    this.next = "next";
    cleanup();
  }
  
  @Override
  public void accumulate(Tuple arg0) throws IOException
  {
    DataBag inputBag = (DataBag)arg0.get(0);
    Iterator<Tuple> it1 = inputBag.iterator();
    Iterator<Tuple> it2 = inputBag.iterator();
    it2.next();
    while(it1.hasNext()){
    	Tuple t1 = TupleFactory.getInstance().newTuple(it1.next().getAll());
    	if(count==0){
    		previous = null;
    	}
    	if(it2.hasNext()){
    		Tuple t2 = TupleFactory.getInstance().newTuple(it2.next().getAll());
    		next = (String) t2.get(0);
    	}
    	else next = null;
    	
    	t1.append(i);
        t1.append(previous);
        t1.append(next);
        previous = (String) t1.get(0);
        //System.out.println(t1.get(0));
        
        outputBag.add(t1);
        /*
        if (count % 1000000 == 0) {
        	outputBag.spill();
          count = 0;
          
        }*/
        i++;
        count++;
    }
  }

  @Override
  public void cleanup()
  {
    this.outputBag = BagFactory.getInstance().newDefaultBag();
    this.i = this.start;
    this.count = 0;
  }

  @Override
  public DataBag getValue()
  {
    return outputBag;
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have only a single field");
      }
      
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      Schema outputTupleSchema = inputTupleSchema.clone();
      outputTupleSchema.add(new Schema.FieldSchema("i", DataType.LONG));
      outputTupleSchema.add(new Schema.FieldSchema("previous", DataType.CHARARRAY));
      outputTupleSchema.add(new Schema.FieldSchema("next", DataType.CHARARRAY));
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
  
	public static void main(String[] args){
		DataBag db;
		DataBag outputdb;
		Tuple t_container = TupleFactory.getInstance().newTuple();
		db = BagFactory.getInstance().newDefaultBag();
		Tuple t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 1");
		db.add(t_new);
		t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 2");
		db.add(t_new);
		t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 3");
		db.add(t_new);
		t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 4");
		db.add(t_new);
		t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 5");
		db.add(t_new);
		t_new = TupleFactory.getInstance().newTuple();
		t_new.append("rad 6");
		db.add(t_new);
		t_container.append(db);
		PreviousNext sc = new PreviousNext("1"); 
		try {
			sc.accumulate(t_container);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		outputdb = sc.getValue();
		System.out.println(outputdb);
	}
  
}
  
