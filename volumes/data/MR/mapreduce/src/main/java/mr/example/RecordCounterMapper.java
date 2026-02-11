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

class RecordCounterMapper {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: RecordCounterMapper <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(RecordCounterMapper.class);
		job.setJobName("Record Counter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RecordMapper.class);
		// Remove the line setting the reducer class
		// job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class RecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		String month = line.substring(19, 21);
		String yearWithMonth = year + "_" + month;

		context.write(new Text(yearWithMonth), new IntWritable(1));
	}
}
