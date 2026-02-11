package mr.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureMapper <input path> <output path>");
			System.exit(-1);
		}

		// Job configurations
		Job job = new Job();
		job.setJarByClass(MaxTemperatureMapper.class);
		job.setJobName("Max temperature");
		job.setNumReduceTasks(0);
		job.setMapperClass(TemperatureMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input & Output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class TemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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
