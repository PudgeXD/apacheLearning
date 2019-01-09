package homework1;

import java.io.IOException;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetMean {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "get mean");
		job.setJarByClass(GetMean.class);
		job.setMapperClass(DivideMapper.class);
		job.setReducerClass(MeanReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class DivideMapper
      extends Mapper<Object, Text, IntWritable, FloatWritable>{

		
		private IntWritable team = new IntWritable();
		private FloatWritable score = new FloatWritable();
		private final int TEAMSIZE = 5;
		
		public DivideMapper(){}
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
				
				String[] pair = value.toString().split(" ");
				this.team.set((Integer.parseInt(pair[0])-1)/this.TEAMSIZE);
				this.score.set(Float.parseFloat(pair[1]));
				
				context.write(this.team, this.score);
		}
	}

	public static class MeanReducer
      extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {
		
		private FloatWritable mean = new FloatWritable();
		
		public MeanReducer() {}
		
		public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) 
				throws IOException, InterruptedException {
			
				float sum = 0;
				int count = 0;
				FloatWritable val;
				for(java.util.Iterator<FloatWritable> i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
					val = (FloatWritable)i$.next();
					count++;
				}
				this.mean.set(sum/count);
				context.write(key, this.mean);
		}
	}

}
