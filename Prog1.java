package employees;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Prog1 {

  //MAPPER CODE	

  public static class Map extends MapReduceBase implements Mapper < LongWritable, Text, Text, IntWritable > {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException {
    	String myval = value.toString();
    	String[] token = myval.split(",");
    	
    	if (token[3].equals("TRUE")) {	
    		output.collect(new Text("Total number of employees eligible for pay raise in their salary: "), one);
    	}
    }
  }

  //REDUCER CODE	
  public static class Reduce extends MapReduceBase implements Reducer < Text, IntWritable, Text, IntWritable > {
    public void reduce(Text key, Iterator < IntWritable > values, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException { 
    	int count = 0 ; 
    	while(values.hasNext()) {
    		count += values.next().get();
    	}
    	output.collect(key, new IntWritable(count));

    }
  }

  //DRIVER CODE
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Prog1.class);
    conf.setJobName("Prog1");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
} 
