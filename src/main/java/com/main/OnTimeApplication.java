package com.main;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.hadoop.fs.FsShell;

import com.example.job.delay.hdfs.HdfsDepartureDelayCountJob;
import com.example.job.delay.mongo.MongoDepartureDelayCountJob;

@SpringBootApplication
@ComponentScan(basePackages="com.example")
public class OnTimeApplication implements CommandLineRunner {
	
	@Autowired
	Configuration conf;
	
	@Autowired
	FsShell fsh;

	@Autowired
	HdfsDepartureDelayCountJob hdfs;
	
	@Autowired
	MongoDepartureDelayCountJob mongo;

	public static void main(String[] args) {
		SpringApplication.run(OnTimeApplication.class, args);
	}
	
	@Value("${job.name:none}")
	String jobName;

	@Override
	public void run(String... args) throws Exception {
		System.out.println(conf);
		fsh.lsr("dataexpo").forEach(e -> System.out.println(e));

		ToolRunner.printGenericCommandUsage(System.out);
		
		System.out.println(Arrays.toString(args));
		System.out.println("job.name=" + jobName);
		
		switch (jobName) {
		case "hdfs":
			ToolRunner.run(conf, hdfs, args);
			break;
		case "mongo":
			ToolRunner.run(conf, mongo, args);
			break;	
		default:
			System.out.println("--job.name=[hdfs|mongo]");
			break;
		}
	}

}
