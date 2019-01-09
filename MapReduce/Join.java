package homework1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(Join.class);
		job.setMapperClass(KeyMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class KeyMapper
      extends Mapper<Object, Text, Text, Text>{
		
		private Text sameAttr = new Text();
		private Text remain = new Text();
		
		public KeyMapper(){}
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
				
				String [] content = value.toString().split(" ");
				if(content.length == 2) {
					this.sameAttr.set(content[0]);
					this.remain.set("A" + " " + content[0] + " " + content[1]);
				} else {
					this.sameAttr.set(content[2]);
					this.remain.set("P" + " " + content[0] + " " + content[1]);
				}
				context.write(this.sameAttr, this.remain);
		}
	}

	public static class JoinReducer
      extends Reducer<Text,Text,Text,Text> {
		
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		
		public JoinReducer() {}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			ArrayList<Object> headList = new ArrayList<>();
			ArrayList<Object> tailList = new ArrayList<>();
			
			for(Text val : values) {
				String [] content = val.toString().split(" ");
				if(content[0].equals("P")) {
					String remain = content[1] + " " + content[2];
					headList.add(remain);
				} else {
					String remain = content[1] + " " + content[2];
					tailList.add(remain);
				}
			}
			for(Object itemHead : headList) {
				for(Object itemTail : tailList) {
					this.keyOut.set((String) itemHead);
					this.valueOut.set((String) itemTail);
					context.write(keyOut, valueOut);
				}
			}
			
		}
	}
}
