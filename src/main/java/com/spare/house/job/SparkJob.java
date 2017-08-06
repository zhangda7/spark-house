package com.spare.house.job;

import org.springframework.beans.factory.annotation.Value;

/**
 * Created by dada on 2017/8/6.
 */
public abstract class SparkJob {

    @Value("${mongo.host}")
    protected String host;

    @Value("${mongo.port}")
    protected int port;

    @Value("${mongo.database}")
    protected String database;

    public abstract void start();

}
