package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives words as key and a set
 * of locations in the form "play name@line number" as the values.
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word.
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  
	  String idx = "";
	  
	  for(Text value:values){
		  // Sum the values up into a long string
		  idx += value.toString() +" ";
	  }
	  
	  context.write(key, new Text(idx));
	  

  }
}
