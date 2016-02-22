package join;

/**
 *
 * @author Siddharth Sujir Mohan
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Join extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
            //creating a JobConf object and assigning a job name for identification purposes
            JobConf conf = new JobConf(getConf(), Join.class);
            conf.setJobName("Join");

            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            //Providing the mapper and reducer class names
            conf.setMapperClass(JoinMapper.class);
            conf.setReducerClass(JoinReducer.class);
          
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
         
            FileInputFormat.addInputPath(conf, inputPath);
            FileOutputFormat.setOutputPath(conf, outputPath);

            JobClient.runJob(conf);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
            // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new Join(),args);
            System.exit(res);
      }
}