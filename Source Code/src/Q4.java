import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q4 {
	
	//businessid, ratings from review table
	public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
		//from ratings
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("::");
			if (mydata.length > 23){
				if("review".compareTo(mydata[22])== 0){		
					Float f = Float.parseFloat(mydata[20]);
						context.write(new Text(mydata[2]),new FloatWritable(f));				
				}
			}	
		
	}
	}
	
	//calculates the average rating for each business id
	//emits businessid, avg rating
	public static class Reducer1 extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		private Map<String, Float> ratingsMap = new HashMap<String,Float>();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count  = 0; // initialize the sum for each keyword
			
			for (FloatWritable val : values) {
				sum += val.get();
				count++;
			}
			
			float average = (float) (sum) / (float) count;
			
			ratingsMap.put(key.toString(), average);
		}
		
		@Override
		protected void cleanup(Context context)
			throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			 for (Map.Entry<String, Float> entry : ratingsMap.entrySet()) { 
				 context.write(new Text(entry.getKey().trim()), new FloatWritable(entry.getValue()));
				 }
			}
	}
	
	
	
	//business_id, B+business_id+full_address+category from business table
		public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("::");
				if (mydata.length > 23){ //business_id should not be null
					if(("business".compareTo(mydata[22])== 0) && ("NaN".compareTo(mydata[3]) != 0) && ("NaN".compareTo(mydata[10]) != 0)){	
						if(mydata[2] != null){
							context.write(new Text(mydata[2]),new Text("B"+mydata[2]+"	"+mydata[3]+"	"+mydata[10]));
						}
					}
			} 
			
		}
		}
		
		//input is the output of job1
		//output is businessid, R+avgrating
		public static class Map3 extends Mapper<LongWritable, Text, Text, Text>{
			Text bid = new Text();
			Text avgrating = new Text();
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String tokens[] = value.toString().split("\\t");
				bid.set(tokens[0].trim());
				avgrating.set("R"+tokens[1]);
				context.write(bid,avgrating);
			}	
		}
		
		//sort by avg rating
		
		//Reduce-side Join
		public static class Reducer2 extends Reducer<Text, Text, Text, FloatWritable> {
			private List<Text> businesstuple = new ArrayList<Text>();
			private List<Float> reviewtuple = new ArrayList<Float>();

			private Map<String, Float> unsortedMap = new HashMap<String,Float>();
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				businesstuple.clear();
				reviewtuple.clear();
				for(Text val: values){
					if(val.charAt(0) == 'R'){
						reviewtuple.add(new Float(val.toString().substring(1)));
					}
					else if(val.charAt(0) == 'B'){
						businesstuple.add(new Text(val.toString().substring(1)));
					}
				}
				
				executeJoinLogic(context);
			}
			private void executeJoinLogic(Context context) throws IOException, InterruptedException {
				if(!businesstuple.isEmpty() && !reviewtuple.isEmpty()){
					for(Text t1: businesstuple){
				/*		String vals[] = t1.toString().split("::");
						Text key = new Text(vals[0]);
						Text business_value = new Text(vals[1]+"::"+vals[2]);
					//	List<String> str = Arrays.asList(vals);*/
						for(Float t2: reviewtuple){ 
						//	Text final_value = new Text(business_value+"::"+t2);
							unsortedMap.put(t1.toString(), t2);
						}
					}
				}
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
		
		

	/*	
		 
		 public static class Map4 extends Mapper<LongWritable, Text, Text, Text>{ 
				
			 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
					
					 String lines[] = value.toString().split("/t");	
					 context.write(new Text(lines[0]),new Text(lines[1]));
				}
			}
		
		//Gives only top10
		 public static class Reducer3 extends Reducer<Text,Text, Text, FloatWritable> {
			 List<String> businessinfo = new ArrayList<String>();
			  FloatWritable avggrating = new FloatWritable();
			  
			 public void reduce(Text key, Iterable<Text> values, Context context)
					 throws IOException, InterruptedException {
				 	for(Text value:values){
				 		String tokens[] = value.toString().split("::");
				 		String mapkey = key+"::"+tokens[0]+"::"+tokens[1];
				 		Float mapvalue = Float.parseFloat(tokens[2]);
				 		unsortedMap.put(mapkey, mapvalue);
				 	}
					 Map<String, Float> sortedMap = new TreeMap<String, Float>(new ValueComparator(unsortedMap));
					 sortedMap.putAll(unsortedMap);
					 int count1 = 0;
					 for (Map.Entry<String, Float> entry : sortedMap.entrySet()) { 
						 count1++;
						 businessinfo.add(entry.getKey());
						 avggrating.set(entry.getValue());
						 context.write(businessinfo, avggrating);
						 if (count1 == 10) {
							 break;
					 	}
					 }
			 }
			 
		 }*/
		 
		 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
					Configuration conf = new Configuration();
					String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
					if (otherArgs.length != 2) {
						System.err.println("Usage: TopTenRateMov reduce side join ");
						System.exit(2);
					}
					// create a job with name "toptenratemov"
					Job job = new Job(conf, "toptenratemovname");
					
				job.setMapperClass(Map1.class);
				job.setReducerClass(Reducer1.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(FloatWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(FloatWritable.class);
				job.setJarByClass(Q4.class);
				
				//set the HDFS path of the input data
				FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
				// set the HDFS path for the output 
				FileOutputFormat.setOutputPath(job, new Path("q4temp1"));
			//	job.waitForCompletion(true);
				   if(job.waitForCompletion(true) == true){
					
						Configuration conf1 = new Configuration();
						Job job1 = new Job(conf1, "join");
						
						job1.setMapOutputKeyClass(Text.class);
						job1.setMapOutputValueClass(Text.class);
						job1.setOutputKeyClass(Text.class);
						job1.setOutputValueClass(FloatWritable.class);
						job1.setJarByClass(Q4.class);
						
						MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Map2.class);
						MultipleInputs.addInputPath(job1, new Path("q4temp1"), TextInputFormat.class, Map3.class);
					
						job1.setReducerClass(Reducer2.class);

						FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
						job1.waitForCompletion(true);
						
				   }
				}
}
