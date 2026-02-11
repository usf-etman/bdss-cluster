package mr.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

class BonusLabPart1 {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BonusLabPart1 <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(BonusLabPart1.class);
		job.setJobName("MaxTempPerMonth");
		job.setNumReduceTasks(12);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BonusLabPart1Mapper.class);
		job.setPartitionerClass(BonusLabPart1Partitioner.class);
		job.setReducerClass(BonusLabPart1Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class BonusLabPart1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		String month = line.substring(19, 21);
		String year_month = year + "\t" + month;
		int airTemperature;
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year_month), new IntWritable(airTemperature));
		}
	}
}

class BonusLabPart1Partitioner<K2, V2> extends Partitioner<Text, IntWritable> {

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String month = key.toString().substring(5);
        return (int) (Integer.parseInt(month) - 1);
    }
}

class BonusLabPart1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}