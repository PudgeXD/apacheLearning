package homework1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

//自己写但来不及Debug
public class Kmeans {
	public static ArrayList<Path> splitMetadate(Path data) throws IOException {
		ArrayList<Path> result = null;
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream fsdis = hdfs.open(data);
		LineReader lineIn = new LineReader(fsdis,conf);
		Text line = new Text();
		ArrayList<Text> content = new ArrayList<Text> ();
		while(lineIn.readLine(line) > 0) {
			content.add(line);
		}
		fsdis.close();
		
		Path mataData = new Path("mataData");
		Path points = new Path("points");
		result.add(mataData);
		result.add(points);
		
		FSDataOutputStream fsdosMataData = hdfs.create(mataData);
		FSDataOutputStream fsdosPoints = hdfs.create(points);
		fsdosMataData.writeChars(content.get(0).toString() + "\n");
		for(int i = 1; i < content.size(); i++) {
			fsdosPoints.writeChars(content.get(i).toString() + "\n");
		}
		fsdosMataData.close();
		fsdosPoints.close();
		return result;
	}
	
	public static ArrayList<ArrayList<Double>> getCenters(Path path){
		ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>(); 
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream fsIn = hdfs.open(path);
			LineReader lineIn = new LineReader(fsIn,conf);
			Text line = new Text();
			while(lineIn.readLine(line) > 0) {
				String record = line.toString();
				String[] fields = record.split(",");
				List<Double> tmpList = new ArrayList<Double> ();
				for(int i = 0; i < fields.length; i++)
					tmpList.add(Double.parseDouble(fields[i]));
				result.add((ArrayList<Double>) tmpList);
			}
			fsIn.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static boolean isChange(Path newCenter) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		List<ArrayList<Double>> content = getCenters(newCenter);
		hdfs.delete(newCenter,true);
		FSDataOutputStream fsdos = hdfs.create(newCenter);
		boolean change = false;
		for(ArrayList<Double> point : content) {
			String XY = point.get(1).toString() + "," + point.get(2) + "\n";
			fsdos.writeChars(XY + "\n");
			if(point.get(0) == 1) change = true;	
		}
		return change;
	}
	
	static boolean finish(Path metaData,Path points,Path oldCenter,Path newCenter) throws IOException,
	InterruptedException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		if(!hdfs.exists(oldCenter)) {
			FSDataInputStream fsdis = hdfs.open(metaData);
			LineReader lineIn = new LineReader(fsdis,conf);
			Text line = new Text();
			lineIn.readLine(line);
			fsdis.close();
			String[] fields = line.toString().split(",");
			int k = Integer.parseInt(fields[0]);
			
			fsdis = hdfs.open(points);
			lineIn = new LineReader(fsdis,conf);
			FSDataOutputStream fsdos = hdfs.create(oldCenter);
			for(int i = 0; i < k; i++) {
				lineIn.readLine(line);
				fsdos.writeChars(line.toString() + "\n");
			}
			fsdis.close();
			fsdos.close();
			return false;
		}else {
			if(isChange(newCenter)) {
				hdfs.delete(oldCenter,true);
				hdfs.rename(newCenter, oldCenter);
				return false;
			} else {
				hdfs.delete(newCenter,true);
				hdfs.rename(oldCenter, new Path("Center"));
				return true;
			}
		}
	}	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		conf.set("mapred.textoutputformat.ignoreseparator","true");
		conf.set("mapred.textoutputformat.separator",",");
		
		Path data = new Path(args[0]);
		ArrayList<Path> tmpList = splitMetadate(data);
		Path metaData = tmpList.get(0);
		Path points = tmpList.get(1);
		Path oldCenter = new Path(args[1]);
		Path newCenter = new Path(args[2]);
		
		FSDataInputStream fsdis = hdfs.open(metaData);
		LineReader lineIn = new LineReader(fsdis,conf);
		Text line = new Text();
		lineIn.readLine(line);
		String [] tmpMeta = line.toString().split(",");
		int k = Integer.parseInt(tmpMeta[0]);
		long count = Long.parseLong(tmpMeta[1]);
		
		conf.setInt("k", k);
		conf.setLong("count", count);
		conf.setStrings("oldCenter", oldCenter.toString());
		
		while(!finish(metaData,points,oldCenter,newCenter)) {
			Job job = Job.getInstance(conf, "Kmeans");
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(ClassifyMapper.class);
			job.setReducerClass(CenterReducer.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, points);
			FileOutputFormat.setOutputPath(job, newCenter);
			System.exit(job.waitForCompletion(true) ? 0 : 1);	
		}
	}
	
	public static class ClassifyMapper
      extends Mapper<Object, Text, Text, Text>{
		
		private Text centerText = new Text();
		private Text pointText = new Text();
		
		public ClassifyMapper(){}
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
				
				pointText.set(value.toString());
				String [] XY = value.toString().split(",");
				ArrayList<Long> point = new ArrayList<Long> ();
				point.add(Long.parseLong(XY[0]));
				point.add(Long.parseLong(XY[1]));
				
				String oldCenterStr = context.getConfiguration().get("oldCenter");
				int k = context.getConfiguration().getInt("k", 0);
				List<ArrayList<Double>> oldCenters = getCenters(new Path(oldCenterStr));
				
				double distance = Double.POSITIVE_INFINITY;
				for(int i = 0; i < k; i++) {
					ArrayList<Double> center = oldCenters.get(i);
					double distanceTmp = 0;
					for(int j = 0; j < point.size(); j++) {
						distanceTmp += Math.pow(center.get(j)-point.get(j),2);
					}
					if(distanceTmp < distance) {
						distance = distanceTmp;
						String centerStr =center.get(0).toString();
						for(int q = 1; q < center.size(); q++) {
							centerStr += "," + center.get(q).toString();
						}
						centerText.set(centerStr);
					}
				}
				context.write(this.centerText, this.pointText);
		}
	}

	public static class CenterReducer
      extends Reducer<Text,Text,IntWritable,Text> {
		
		private IntWritable modifyFlag = new IntWritable();
		private Text XY = new Text();
		
		public CenterReducer() {}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			ArrayList<Long>accument = new ArrayList<Long> ();
			boolean init = true;
			String [] dimensionTest = key.toString().split(",");
			int dimension = dimensionTest.length;
			
			long count = 0;
			for(Text val : values) {
				String [] point = val.toString().split(",");
				count++;
				if(init) {
					for(int i=0; i < dimension; i++) {
						accument.add(Long.parseLong(point[i]));
					}
					init = false;
				}else {
					for(int i=0; i < dimension; i++) {
						long tmp = accument.get(i);
						tmp += Long.parseLong(point[i]);
						accument.set(i, tmp);
					}
				}	
			}
			
			ArrayList<Double> newCenterDouble = new ArrayList<Double> ();
			for(int i = 0; i < dimension; i++) {
				long tmp = accument.get(i);
				double mean = (double)tmp/count;
				newCenterDouble.add(mean);
			}
			String newCenter = newCenterDouble.get(0).toString();
			for(int i = 1; i < dimension; i++) {
				newCenter += "," + newCenterDouble.get(i).toString();
			}
			
			String oldCenter = key.toString();
			modifyFlag.set(0);
			if(oldCenter.equals(newCenter)) {
				modifyFlag.set(0);
			} else {
				modifyFlag.set(1);
			}
			XY.set(newCenter);
			context.write(this.modifyFlag,this.XY);	
		}
	}
}
