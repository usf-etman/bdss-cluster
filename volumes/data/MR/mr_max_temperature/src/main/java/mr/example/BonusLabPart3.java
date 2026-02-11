package mr.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class BonusLabPart3 {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BonusLabPart3 <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(BonusLabPart3.class);
		job.setJobName("TrendAnalysis");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BonusLabPart3Mapper.class);
		job.setReducerClass(BonusLabPart3Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class BonusLabPart3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String month = line.split("\t")[1];
		int temperature = Integer.parseInt(line.split("\t")[2]);
		int prev_temperature = Integer.parseInt(line.split("\t")[3]);
		
		int diff = prev_temperature == 9999 ? 9999 : temperature - prev_temperature;
		int c = 0;
		if (diff > 0 && diff < 9999) { c = 1; }
		else if (diff < 0) { c = -1; }

		context.write(new Text(month), new IntWritable(c));
	}
}

class BonusLabPart3Reducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String trend = "";
		for (IntWritable value : values) {
			sum += value.get();
		}
		if(sum > 0) { trend = "Warming"; }
		else if (sum < 0) { trend = "Cooling"; }
		else { trend = "No Change"; }
		context.write(key, new Text(trend));
	}
}