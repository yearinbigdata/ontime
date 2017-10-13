package com.example.job.delay.mongo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.stereotype.Component;

import com.example.persistence.DepartureDelayRepository;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;

@Component
public class MongoDepartureDelayCountJob extends Configured implements Tool {
	
	@Autowired
	DepartureDelayRepository repo;
	
	@Autowired
	Configuration conf;
	
	@Autowired
	FsShell fsh;
	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		conf.set("mongo.output.uri", "mongodb://bigdata:27017/ontime.departure_delay");
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		job.setJarByClass(MongoDepartureDelayCountJob.class);
		
//		FileInputFormat.setInputPaths(job, "dataexpo/1987_nohead.csv");
//		FileInputFormat.addInputPaths(job, "dataexpo/1988_nohead.csv");
//		FileInputFormat.addInputPaths(job, "dataexpo/1989_nohead.csv");
//		FileInputFormat.addInputPaths(job, "dataexpo/1990_nohead.csv");
		
		FileInputFormat.addInputPaths(job, "dataexpo");
		
		job.setInputFormatClass(TextInputFormat.class);	// 생략가능
		
		job.setMapperClass(MongoDepartureDelayCountMapper.class);
		job.setMapOutputKeyClass(Text.class);				// 생략가능
		job.setMapOutputValueClass(IntWritable.class);	// 생략가능
		
		job.setReducerClass(MongoDepartureDelayCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);
		
		job.setOutputFormatClass(MongoOutputFormat.class);// 생략가능
		
//		String outputDir = "dataexpo_out/1988";
//		String outputDir = "dataexpo_out/1990";
		String outputDir = "dataexpo_out/total";
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		MultipleOutputs.addNamedOutput(job, "xxx", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "yyy", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);
		
		repo.deleteAll();
		
		job.waitForCompletion(true);
		
		repo.findAll().forEach(e -> System.out.println(e));
		
		return 0;
	}

}
