package com.example.job.delay.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirLinePerformaceParser;
import com.example.parser.DelayCounters;

public class HdfsDepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final IntWritable one = new IntWritable(1);
	
	Text outputKey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirLinePerformaceParser parser = new AirLinePerformaceParser(value);
		
		if (parser.isDepartureDelayAvaiable()) {
			if (parser.getDepartureDelayTime() > 0) {
				outputKey.set(parser.getYear() + "");
				context.write(outputKey, one);
				context.getCounter(DelayCounters.delay_departure).increment(1);
			} else if (parser.getDepartureDelayTime() == 0)
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			else
				context.getCounter(DelayCounters.early_departure).increment(1);
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);;
		}
	}

}
