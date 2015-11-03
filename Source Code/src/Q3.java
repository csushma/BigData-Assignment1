
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3 {

	//businessid, rating
	public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				//from ratings
				
				String[] mydata = value.toString().split("::");
				if (mydata.length > 23){
					if("review".compareTo(mydata[22])== 0){		
						Float f = Float.parseFloat(mydata[20]);
							context.write(new Text(mydata[2]),new FloatWritable(f));				
					}
				}	
						
			}
		}
//business id,avgrating
	public static class Reducer1 extends Reducer<Text,FloatWritable,Text,FloatWritable> {

		 private Map<String, Float> unsortedMap = new HashMap<String,Float>();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count  = 0; // initialize the sum for each keyword
			
			for (FloatWritable val : values) {
				sum += val.get();
				count++;
			}
			
			float average = (float) (sum) / (float) count;
			
			 unsortedMap.put(key.toString(), average);
		}
		
		private static Map<String, Float> sortByComparator(Map<String, Float> unsortedMap2) {
			 
			// Convert Map to List
			List<Map.Entry<String, Float>> list = 
				new LinkedList<Map.Entry<String, Float>>(unsortedMap2.entrySet());
	 
			// Sort list with comparator, to compare the Map values
			Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
				public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
					return (o2.getValue()).compareTo(o1.getValue());
				}
			});
	 
			// Convert sorted map back to a Map
			Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
			for (Iterator<Map.Entry<String, Float>> it = list.iterator(); it.hasNext();) {
				Map.Entry<String, Float> entry = it.next();
				sortedMap.put(entry.getKey(), entry.getValue());
			}
			return sortedMap;
		}
		
		@Override
		protected void cleanup(Context context)
			throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			 Map<String, Float> sortedMap = sortByComparator(unsortedMap);
			 int count1 = 0;
			 for (Map.Entry<String, Float> entry : sortedMap.entrySet()) { 
				 count1++;
				 context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
				 if(count1 == 10){
					 break;
				 }
			 	}
			 }
		}
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: TopTenRateMov <ratings> <movies> ");
				System.exit(2);
			}
			// create a job with name "toptenratemov"
			Job job = new Job(conf, "toptenratemovname");
			job.setJarByClass(Q3.class);
			
		/*	final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
	        job.addCacheFile(new URI(NAME_NODE
			    + "/user/hue/users/users.dat"));*/
			
			job.setMapperClass(Map1.class);
			job.setReducerClass(Reducer1.class);
			//job.setCombinerClass(Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			// set output key type 
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(FloatWritable.class);
			
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			
			job.waitForCompletion(true);
			//Wait till job completion
		/*	if(job.waitForCompletion(true) == true){
				
				// create a job with name "toptenratemov"
				Job job2 = new Job(conf, "toptenratemov2");
				
				job2.setJarByClass(Q3.class);
				job2.setReducerClass(Reducer2.class);
				job2.setMapperClass(Map2.class);
				
				MultipleInputs.addInputPath(job2,  new Path("datatemp"), TextInputFormat.class,
						Map2.class);
				
				 // Set the outputs for the Map
		        job2.setMapOutputKeyClass(NullWritable.class);
		        job2.setMapOutputValueClass(Text.class);

				// set output key type 
				job2.setOutputKeyClass(Text.class);
				// set output value type
				job2.setOutputValueClass(FloatWritable.class);
				
				//set the HDFS path of the input data
				// set the HDFS path for the output 
				FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
				job2.waitForCompletion(true);
				
			}*/
		}

}
