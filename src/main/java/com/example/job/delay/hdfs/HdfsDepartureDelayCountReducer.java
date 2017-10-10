package com.example.job.delay.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class HdfsDepartureDelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	MultipleOutputs<Text, IntWritable> mos;

	IntWritable outValue = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		outValue.set(sum);

		mos.write("xxx", key, outValue);
		mos.write("yyy", key, outValue);
		context.write(key, outValue);
		context.getCounter("XXX", "total").increment(1);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}
