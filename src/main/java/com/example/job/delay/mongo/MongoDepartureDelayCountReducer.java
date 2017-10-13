package com.example.job.delay.mongo;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class MongoDepartureDelayCountReducer extends Reducer<Text, IntWritable, Text, BSONWritable> {

	MultipleOutputs<Text, BSONWritable> mos;

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

//		mos.write("xxx", key, outValue);
//		mos.write("yyy", key, outValue);
		
		BasicBSONObject object = new BasicBSONObject();
		object.put("delay", sum);
		BSONWritable value = new BSONWritable();
		value.setDoc(object);
		
		context.write(key, value);
		context.getCounter("XXX", "total").increment(1);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}
