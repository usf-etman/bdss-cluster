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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

class RecordCounterMapperCombinerPartitionerReducer {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: RecordCounterMapperCombinerPartitionerReducer <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(RecordCounterMapperCombinerPartitionerReducer.class);
		job.setJobName("Record Counter");
		job.setNumReduceTasks(12);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RecordMapper3.class);
		job.setCombinerClass(RecordCombiner3.class);
		job.setPartitionerClass(RecordPartitioner.class);
		job.setReducerClass(RecordReducer3.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class RecordMapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		String month = line.substring(19, 21);
		String yearWithMonth = year + "_" + month;

		context.write(new Text(yearWithMonth), new IntWritable(1));
	}
}

class RecordPartitioner<K2, V2> extends Partitioner<Text, IntWritable> {

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        return (int) (Integer.parseInt(key.toString().substring(5)) - 1);
    }
}

class RecordCombiner3 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int records = 0;
		for (@SuppressWarnings("unused") IntWritable value : values) {
			records++;
		}
		context.write(key, new IntWritable(records));
	}
}

class RecordReducer3 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int records = 0;
		for (IntWritable value : values) {
			records += value.get();
		}
		context.write(key, new IntWritable(records));
	}
}