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

class BonusLab {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: BonusLab <input path> <job1 output path> <job2 output path> <job3 output path>");
			System.exit(-1);
		}

		//////////////////////////////////// Job 1
		Job job1 = new Job();
		job1.setJarByClass(BonusLab.class);
		job1.setJobName("MaxTempPerMonth");
		job1.setNumReduceTasks(12);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(BonusLabMapper1.class);
		job1.setPartitionerClass(BonusLabPartitioner.class);
		job1.setReducerClass(BonusLabReducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		if (!job1.waitForCompletion(true)) { System.exit(1); }

		//////////////////////////////////// Job 2
		Job job2 = new Job();
		job2.setJarByClass(BonusLab.class);
		job2.setJobName("YearlyChange");
		job2.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.setMapperClass(BonusLabMapper2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		if (!job2.waitForCompletion(true)) { System.exit(1); }

		//////////////////////////////////// Job 3
		Job job3 = new Job();
		job3.setJarByClass(BonusLab.class);
		job3.setJobName("TrendAnalysis");

		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		job3.setMapperClass(BonusLabMapper3.class);
		job3.setReducerClass(BonusLabReducer3.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}

class BonusLabMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
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

class BonusLabReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}

class BonusLabPartitioner<K2, V2> extends Partitioner<Text, IntWritable> {

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String month = key.toString().substring(5);
        return (int) (Integer.parseInt(month) - 1);
    }
}

class BonusLabMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	String prev_year_temp = "9999"; // NULL

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.split("\t")[0];
		String month = line.split("\t")[1];
		String temperature = line.split("\t")[2];

		context.write(new Text(year), new Text(month+"\t"+temperature+"\t"+prev_year_temp));
		prev_year_temp = temperature;
	}
}

class BonusLabMapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {
	String prev_year_temp = ""; // NULL

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

class BonusLabReducer3 extends Reducer<Text, IntWritable, Text, Text> {

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