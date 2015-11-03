
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q5{
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		//private Text userid = new Text();
	//	private Text reviewText = new Text();  
		private String strbusinessLoc = "";
		private Text txtMapOutputKey = new Text("");
		private Text txtMapOutputValue = new Text("");

		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		}
		
		public static HashMap<String,String> businessMap = new HashMap<String, String >();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);
			
			businessMap = new HashMap<String, String >();
			Configuration conf = context.getConfiguration();
			Path part=new Path("hdfs://sandbox.hortonworks.com:8020/user/hue/input/business.csv");//Location of file in HDFS
		//	Path part=new Path("hdfs://cshadoop1/allyelpdata/business.csv");//Location of file in HDFS
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		            //System.out.println(line);
		            String[] arr=line.split("::");
		            businessMap.put( arr[2].trim(), arr[3].trim()); //business id 
		            line=br.readLine();
		    }
		    }
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from review
			context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

			if (value.toString().length() > 0) 
			{
				String arrYelpData[] = value.toString().split("::");
				
				if(arrYelpData.length>23 && ("review".equalsIgnoreCase(arrYelpData[22]) || "business".equalsIgnoreCase(arrYelpData[22])) )
				{
					strbusinessLoc = businessMap.get(arrYelpData[2].toString());
					if(strbusinessLoc != null && strbusinessLoc.toLowerCase().contains("stanford") && arrYelpData[1].toString() != null && !("NaN").equals(arrYelpData[8].toString()) && !("NaN").equals(arrYelpData[8].toString()))
					//if(strbusinessLoc != null)
					{	
						txtMapOutputKey.set(arrYelpData[8].toString());
						txtMapOutputValue.set(arrYelpData[1].toString());
						//txtMapOutputValue.set(strbusinessLoc);
					}	
					if(!("").equals(txtMapOutputKey.toString()) && !("").equals(txtMapOutputKey.toString()))	{
						context.write(txtMapOutputKey, txtMapOutputValue);	
					txtMapOutputKey = new Text("");
					txtMapOutputValue = new Text("");
					strbusinessLoc = "";	
	}
				}
			}
		}
	}
// Driver program
			public static void main(String[] args) throws Exception {
				Configuration conf = new Configuration();
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
				if (otherArgs.length != 2) {
					System.err.println("Usage: MoviesRated map side join");
					System.exit(2);
				}
				
				Job job = Job.getInstance(new Configuration());
				job.setJobName("Map-side join with text lookup file in DCache");
			//	 final String NAME_NODE = "hdfs://localhost:9000";
		    //    job.addCacheFile(new URI(NAME_NODE
			//	    + "/user/hduser/"+otherArgs[1]+"/users.dat"));
		//		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
			//        job.addCacheFile(new URI(NAME_NODE
				//	    + "/user/hue/input/business.csv"));
			      
				job.setJarByClass(Q5.class); 
			    job.setMapperClass(Map.class);
				job.setNumReduceTasks(0);
	
				// set output key type 
				job.setOutputKeyClass(Text.class);
				// set output value type
				job.setOutputValueClass(Text.class);
				
				//set the HDFS path of the input data
				FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
				// set the HDFS path for the output 
				FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
				
				//Wait till job completion
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
	
}