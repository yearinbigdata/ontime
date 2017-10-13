package com.example.job.delay.mongo;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.domain.DepartureDelay;
import com.example.persistence.DepartureDelayRepository;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoDepartureDelayJobTest {

	@Autowired
	DepartureDelayRepository repo;

	@Test
	public void testFindAll() {
		System.out.println(repo);
		
		repo.findAll().forEach(e-> System.out.println(e));
	}
	
	@Test
	public void testInsert() {
		System.out.println(repo);
		
		for (int i=0; i<5; i++) {
			DepartureDelay delay = new DepartureDelay();
			delay.setYear("197" + i);
			delay.setDelay(1700 + i);
			repo.save(delay);
		}
		
		repo.findAll().forEach(e-> System.out.println(e));
	}
	
	@Autowired
	Configuration conf;
	
	@Autowired
	MongoDepartureDelayCountJob job;
	
	@Test
	public void testJob() throws Exception {
		ToolRunner.run(conf, job, null);
	}

}
