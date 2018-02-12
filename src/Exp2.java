
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Exp2 {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/user/jkuczek/input"; 
		//String input = "/cpre419/patents.txt"; 
		String temp1 = "/user/jkuczek/lab3/exp2/temp1";
		//String temp2 = "/user/jkuczek/lab3/exp2/temp2";
		String output = "/user/jkuczek/lab3/exp2/output/"; 

		// The number of reduce tasks 
		int reduce_tasks = 1; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Graph Program Round One");

		// Attach the job to this Exp1
		job_one.setJarByClass(Exp2.class);

		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		// The class that provides the map method
		job_one.setMapperClass(Exp2_Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Exp2_Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));
		
		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		Job job_two = Job.getInstance(conf, "Graph Program Round Two");
		job_two.setJarByClass(Exp2.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Exp2_Map_Two.class);
		job_two.setReducerClass(Exp2_Reduce_Two.class);
				
		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp1));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);
//		
//		// Create job for round 3
//		// The output of the previous job can be passed as the input to the next
//		// The steps are as in job 1
//		Job job_three = Job.getInstance(conf, "Graph Program Round Three");
//		job_three.setJarByClass(Exp1.class);
//		job_three.setNumReduceTasks(reduce_tasks);
//
//		// Should be match with the output datatype of mapper and reducer
//		job_three.setMapOutputKeyClass(IntWritable.class);
//		job_three.setMapOutputValueClass(Text.class);
//		job_three.setOutputKeyClass(Text.class);
//		job_three.setOutputValueClass(Text.class);
//
//		// If required the same Map / Reduce classes can also be used
//		// Will depend on logic if separate Map / Reduce classes are needed
//		// Here we show separate ones
//		job_three.setMapperClass(Map_Three.class);
//		job_three.setReducerClass(Reduce_Three.class);
//				
//		job_three.setInputFormatClass(TextInputFormat.class);
//		job_three.setOutputFormatClass(TextOutputFormat.class);
//		
//		// The output of previous job set as input of the next
//		FileInputFormat.addInputPath(job_three, new Path(temp2));
//		FileOutputFormat.setOutputPath(job_three, new Path(output));
//
//		// Run the job
//		job_three.waitForCompletion(true);
	}// end function main

	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Exp2_Map_One extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text w1 = new Text();
		private Text w2 = new Text();
		
		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
						
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(line);

			while (tokens.hasMoreTokens()) {
				w1.set(tokens.nextToken());
				w2.set(tokens.nextToken());
				
				if(!w1.equals(w2)) {
					context.write(w1, w2);
					context.write(w2, w1);
				}// end if two vertices are not equal
			} // End while
		}// end function map 
	}// end class Map_One
	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Exp2_Reduce_One extends Reducer<Text, Text, Text, Text> {
		
		private Text w1 = new Text();
		private Text w2 = new Text();
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			w1.set(key);
			w2.set(toList(values));
			context.write(w1, w2);
		}// end function reduce
		
		public String toList(Iterable<Text> values) {
			String result = "";
			for(Text value : values) {
				result += value + ",";
			}// end foreach loop over values
			return result.substring(0, result.length()-1);
		}// end function toList
		
	}// end class Reduce_One

	// The second Map Class
    public static class Exp2_Map_Two extends Mapper<LongWritable, Text, Text, Text> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted
        	String[] data = value.toString().split("\t");
 
        	// pull relevant vertex data from input
        	String vertex = data[0];
        	ArrayList<String> neighbors = toList(data[1]);
        	
        	for(int i = 0; i < neighbors.size(); i++) {
        		// loop through the neighbor list
        		String v = neighbors.get(i);
        		context.write(new Text(v), new Text(vertex+"-"+String.join(",", neighbors)));
        	}// end for loop over all neighbors
        	
        } // end function map
        
        private ArrayList<String> toList(String s) {
        	ArrayList<String> list = new ArrayList<String>(Arrays.asList(s.split(",")));
        	Collections.sort(list);
        	return list;
        }// end function convert
        
    }// end class Map_Two

    // The second Reduce class
    public static class Exp2_Reduce_Two extends Reducer<Text, Text, Text, Text> {
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	System.out.print("\nVertex: " + key);
        	
        	for(Text value : values) {
        		// parse the input data from Map_Two
        		String[] data = value.toString().split("-");
        		String vertex = data[0];
        		ArrayList<String> neighbors = toList(data[1]);
        		
        		System.out.println("\nKEY: " + vertex);
        		System.out.print("VALUES: ");
        		for(String v : neighbors) {
        			if(!v.equals(key.toString())) {
        				System.out.print(v);
        			}// end if vertex not equal (don't include duplicates
        		}// end for loop over all 
        		
        	}// end foreach loop over values
        }// end function reduce
        
        private ArrayList<String> toList(String s) {
        	ArrayList<String> list = new ArrayList<String>(Arrays.asList(s.split(",")));
        	Collections.sort(list);
        	return list;
        }// end function convert
        
    }// end class Reduce_Two
    
    // The third Map Class
    public static class Exp2_Map_Three extends Mapper<LongWritable, Text, IntWritable, Text> {
    			
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// output from the reduce method is tab delimitted
        	String[] data = value.toString().split("\t");
        	IntWritable count = new IntWritable(Integer.parseInt(data[0]));
        	context.write(count, new Text(data[1]));
        } // end function map
        
    }// end class Map_Three
    
    // The third Reduce class
    public static class Exp2_Reduce_Three extends Reducer<IntWritable, Text, IntWritable, Text> {
    	
    	private Text word = new Text();
    	LinkedHashMap<Integer, String> queue = new LinkedHashMap<Integer, String>()
        {
		   static final long serialVersionUID=42L;
		   
           @Override
           protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest)
           {
              return this.size() > 10;   
           }
        };
		
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	// add elements to the queue, since reduce sorts by key in ascending order,
        	// the last items to be added to the queue will be the top 10 bigrams
            for (Text val : values) {
            	if(val != null) {
            		queue.put(new Integer(key.get()), val.toString());
            	}// end if value is not null
            }// end for loop adding all elements to priority queue
        	//context.write(null, new Text("test"));
        }// end function reduce
      
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
      	  // print out the contexts of the queue once the task is complete
      	  String result = "";
      	  int i = 9;
      	  String[] s = new String[10];
      	
          for (Integer count : queue.keySet()){
          	String vertex = queue.get(count).toString();
          	s[i--] = "Patent: " + vertex + " - Significance: " + count + "\r\n";  
          }// end for loop over the LinkedHashMap
          
          for(i = 0; i < 10; i++) {
          	result += i+1 + ": " + s[i];
          }// end for loop printing out results 
          
          word.set(result);
          context.write(null, word);
        }// end cleanup function
        
    }// end class Reduce_Three

}// end class Exp1
