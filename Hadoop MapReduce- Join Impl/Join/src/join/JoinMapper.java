package join;

/**
 *
 * @author Siddharth Sujir Mohan
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class JoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
      
      private Text keyText = new Text();
     
      
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
      {
         
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            String join="";
            join+=tokenizer.nextToken();
            String key2=tokenizer.nextToken();
     
            while (tokenizer.hasMoreTokens())
            {
               join+=" "+tokenizer.nextToken();
                           
            }
            keyText.set(key2);
            Text val=new Text();
            val.set(join);
            output.collect(keyText, val);
       }
}