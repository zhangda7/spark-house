package com.spare.house;

import com.spare.house.job.CurrentHouseJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by dada on 2017/8/6.
 */
@SpringBootApplication
public class Application {

    private static Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    CurrentHouseJob estateTrendJob;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        logger.info("App started");

    }

}
