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

class BonusLabFail {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BonusLabFail <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(BonusLabFail.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BonusLabFailMapper.class);
		job.setReducerClass(BonusLabFailReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class BonusLabFailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String month = line.split("\t")[1];
		int temperature = Integer.parseInt(line.split("\t")[2]);

		context.write(new Text(month), new IntWritable(temperature));
	}
}

class BonusLabFailReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		boolean isFirst = true;
		int c = 0;
		int previous = Integer.MIN_VALUE;
		String trend = "";

		for (IntWritable value : values) {
            int current = value.get();
			if(isFirst) { isFirst = false; }
			else {
				if(current > previous) { c++; }
				else if(current < previous) { c--; }
			}
			trend += current + " ";
			previous = current;
		}

		// if(c>0) { trend = "Warming"; }
		// else if (c<0) {trend = "Cooling"; }
		// else { trend = "No Change"; }

		context.write(key, new Text(trend));
	}
}