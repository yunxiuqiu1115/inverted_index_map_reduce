package stubs;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */
	  
	  String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();// get to know which file is being read
	  
	  String line = value.toString();

	  String[] indexKeys = line.toLowerCase().split("\t");// seperate the line number and content


	  
	  for(int i=1;i<indexKeys.length;i++){
		  for(String word:indexKeys[i].split("\\W+"))// omitting those punctuation
			  {
			  if(word.length()>0)
		  {
			  context.write(new Text(word),new Text(fileName+"@"+indexKeys[0]));//input a word and its corresponding filename and line number
			  
		  }
			  }
	  }
			  

  } 
}
