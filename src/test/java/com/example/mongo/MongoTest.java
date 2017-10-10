package com.example.mongo;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.persistence.DepartureDelayRepository;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoTest {

	@Autowired
	DepartureDelayRepository repo;

	@Test
	public void testAsc() {
		System.out.println(repo);
		
		repo.findAll().forEach(e -> {
			System.out.println(e);
		});
	}
	
	@Test
	public void testDesc() {
		repo.findAll(Sort.by(Direction.DESC, "year")).forEach(e -> {
			System.out.println(e);
		});
		
	}

}
