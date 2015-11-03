import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from ratings
			
			String[] mydata = value.toString().split("::");
			if (mydata.length > 23){
				if("business".compareTo(mydata[22])== 0){
					if(mydata[3].contains("Palo")== true){
						context.write(new Text(mydata[3]),new Text(mydata[2]));
					}
				}
			}	
					
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		private Text myKey = new Text();
		//note you can create a list here to store the values
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val.toString());
				myKey.set(key.toString());
				context.write(result, null);
			}
		}		
	}
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
			if (otherArgs.length < 2) {
				System.err.println("Usage: Yelptest2 <in> <out>");
				System.exit(2);
			}
			
			
			Job job = new Job(conf, "JoinYelp");
			job.setJarByClass(Q2.class);
			
			MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map.class );

			job.setReducerClass(Reduce.class);

	//		job.setCombinerClass(Reduce.class);
			
			// set output key type 
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(Text.class);
	
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			
			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
