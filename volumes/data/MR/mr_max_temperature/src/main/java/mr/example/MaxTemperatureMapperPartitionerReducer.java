package mr.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTemperatureMapperPartitionerReducer {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureMapperPartitionerReducer <input path> <output path>");
			System.exit(-1);
		}

		// Job configurations
		Job job = new Job();
		job.setJarByClass(MaxTemperatureMapperPartitionerReducer.class);
		job.setJobName("Max temperature");
		job.setNumReduceTasks(2);
		job.setMapperClass(TemperatureMapper3.class);
		job.setPartitionerClass(TemperaturePartitioner.class);
		job.setReducerClass(TemperatureReducer3.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input & Output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class TemperatureMapper3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// get value
		String line = value.toString();

		// Ignore headers
		if (line.startsWith("STATION")) { return; }
		
		// Parse fields
		String[] fields = line.split("\\|");
		String year = fields[2].substring(0, 4);
		double temperature = Double.parseDouble(fields[6]);
		int qualityCode = Integer.parseInt(fields[8]);

		// output record if valid quality code
		if (qualityCode == 0 || qualityCode == 1) {
			context.write(new Text(year), new DoubleWritable(temperature));
		}
	}
}

class TemperaturePartitioner<K2, V2> extends Partitioner<Text, DoubleWritable> {

	public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
        return (int) (Integer.parseInt(key.toString()) - 1901);
    }
}

class TemperatureReducer3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double maxValue = Double.MIN_VALUE;
		for (DoubleWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new DoubleWritable(maxValue));
	}
}