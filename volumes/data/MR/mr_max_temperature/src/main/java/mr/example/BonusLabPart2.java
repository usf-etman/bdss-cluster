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

class BonusLabPart2 {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BonusLabPart2 <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(BonusLabPart2.class);
		job.setJobName("YearlyChange");
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BonusLabPart2Mapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class BonusLabPart2Mapper extends Mapper<LongWritable, Text, Text, Text> {
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