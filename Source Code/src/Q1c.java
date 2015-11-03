

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1c {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("::");
			if (mydata.length > 23){
				if("user".compareTo(mydata[22])== 0){	
					context.write(new Text("user"),one);
				}
			}	
					
		}
	}
		public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int sum = 0; // initialize the sum for each keyword
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);

				context.write(key, result); // create a pair <keyword, number of occurences>
			}
		}
		
	
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: Question1 <in> <out>");
				System.exit(2);
			}

		
			Job job = new Job(conf, "JoinYelp"); 
			job.setJarByClass(Q1c.class); 
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// uncomment the following line to add the Combiner 

			job.setCombinerClass(Reduce.class);
			// set output key type 
			job.setOutputKeyClass(Text.class);
			// set output value type 
			job.setOutputValueClass(IntWritable.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
