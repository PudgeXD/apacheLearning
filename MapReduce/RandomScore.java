package homework1;

import java.io.IOException;
import java.util.Random;
//import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RandomScore {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "random socre");
		job.setJarByClass(RandomScore.class);
		job.setMapperClass(ProduceMapper.class);
		job.setReducerClass(NullReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class ProduceMapper
      extends Mapper<Object, Text, IntWritable, IntWritable>{

		private IntWritable id = new IntWritable();
		private IntWritable score = new IntWritable();
		
		public ProduceMapper(){}
		
		public void map(Object key, Text value, 
				Mapper<Object, Text, IntWritable, IntWritable>.Context context) 
				throws IOException, InterruptedException {
				
				int tmp = Integer.parseInt(value.toString());
				this.id.set(tmp);
				Random random = new Random();
				this.score.set(random.nextInt(101));
				context.write(this.id, this.score);
		}
	}

	public static class NullReducer
      extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		
		public NullReducer() {}
		public void reduce(IntWritable key, IntWritable value, 
				Reducer<IntWritable,IntWritable,IntWritable,IntWritable>.Context context) 
				throws IOException, InterruptedException {
				context.write(key, value);
		}
	}

}
