package com.project.snaptrade;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SnaptradeApplication {

	public static void main(String[] args) {
		SpringApplication.run(SnaptradeApplication.class, args);
	}

}
