package com.example.job.delay.hdfs;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.job.delay.hdfs.HdfsDepartureDelayCountJob;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsDepartureDelayJobTest {
	
	@Autowired
	FsShell fsh;

	@Test
	public void testFindAll() {
		System.out.println(fsh);
		
		fsh.ls("dataexpo_out").forEach(e -> System.out.println(e));
	}
	
	@Autowired
	Configuration conf;
	
	@Autowired
	HdfsDepartureDelayCountJob job;
	
	@Test
	public void testJob() throws Exception {
		ToolRunner.printGenericCommandUsage(System.out);
//		String config="/opt/hadoop2/etc/hadoop/";
//		String core = config + "core-site.xml";
//		String hdfs = config + "hdfs-site.xml";
//		String mapred = config + "mapred-site.xml";
//		String yarn = config + "yarn-site.xml";
//		
//		String[] args = {"-conf", core + "," + hdfs + "," + mapred + "," + yarn};
		ToolRunner.run(conf, job, null);
	}

}
