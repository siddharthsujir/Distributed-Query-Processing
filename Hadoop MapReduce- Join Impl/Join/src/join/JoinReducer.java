package join;

/**
 *
 * @author Siddharth Sujir Mohan
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class JoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
    private Text tuple = new Text();
      //reduce method accepts the Key Value pairs from mappers, and performs join based on the key
     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
      {
    HashMap<String, ArrayList<String>> relation_records = new HashMap<String, ArrayList<String>>();
    String table="";
    ArrayList<String> row_value;
    String join = "";
    String value = "";
      // For each value for the same key
      while(values.hasNext())   
               
      {
        Text val=values.next();
    	value = val.toString();
        int index = value.indexOf(" ");
        table = value.substring(0,index);
        value = value.substring(index+1, value.length());
        
       // To find if the table keys match
        if(relation_records.containsKey(table))
        { 	
        	row_value = relation_records.get(table);
        	row_value.add(value); 
        	relation_records.put(table, row_value);
        }
        else
        {
           row_value = new ArrayList<String>(); 
           row_value.add(value); 
       	   relation_records.put(table, row_value);
        }
        
         
      }
      
      Set<String> table_names_set = relation_records.keySet();
      if(table_names_set.size() >1)
      {
    	 
         String[] table_names_arr = new String[table_names_set.size()];
         table_names_set.toArray(table_names_arr);
         String tableId1 = table_names_arr[0];
         String tableId2 = table_names_arr[1];
         ArrayList<String> table1Rows = relation_records.get(tableId1);
         ArrayList<String> table2Rows = relation_records.get(tableId2);
         
                 
         for(String table1:table1Rows)
         {
           for(String table2:table2Rows)
            {
             join = table1+" "+table2;
             tuple.set(join);
              output.collect(key, tuple);
            }
          }
         }
      }
    
}