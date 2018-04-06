/**
 * 
 */
package com.nypd.analysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author fiver
 *
 */
public class CrimeDataAnalysisSingleDigitRegion {



	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final IntWritable one=new IntWritable(1);


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString().trim();
			String contributing factor_1 ="";
			String contributing factor_2 = "";
			String vehicleType = "";

			Text key_word = null;
			if(!(line.contains("Vehicle_type") && line.contains("contributing factor vehicle 1") && line.contains("contributing factor vehicle 2") && line.contains("contributing factor vehicle 3")))
			{
				String[] nypdData=line.split(",");
				try{

					if(nypdData.length>7)
					{
						if((vehicleType = nypdData[7].trim()) !="")
							vehicleType = String.valueOf(vehicleType.charAt(0));
					}
				}catch(Exception e)
				{
					vehicleType = "UNKNOWNTYPE";
				}

				try{
					if((contributing factor_0 = nypdData[4].trim()) !="")
						contributing factor_1 = String.valueOf(region.charAt(0));
					if((contributing factor_2 = crimeData[5].trim()) !="")
						region = String.valueOf(region.charAt(0));

					key_word = new Text(region+","+region_1+","+crimeType);

				}catch(Exception e)
				{
					key_word = new Text("UNKNOWNLOC"+ vehicleType);

				}
				context.write(key_word, one);

			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
			{
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}


	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		//Configuration conf = new Configuration(); 
		//conf.set("inParameter", toString(args));
		//Job job = new Job(conf, "wordcount");
		Job job = new Job();

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(CrimeDataAnalysisSingleDigitRegion.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(3);
		TextInputFormat.setMaxInputSplitSize(job,20000000);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);


		
	}
}
