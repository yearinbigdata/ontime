package com.example.fs;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FsTest {

	@Autowired
	Configuration conf;
	
	@Autowired
	FsShell fsh;

	@Test
	public void test() {
		System.out.println(conf);
		System.out.println(fsh);

		fsh.lsr("dataexpo").forEach(e -> System.out.println(e));
		conf.forEach(e -> System.out.println(e));
	}

}
