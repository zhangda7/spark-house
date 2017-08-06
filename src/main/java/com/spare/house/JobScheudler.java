package com.spare.house;

import com.spare.house.dao.MongoDao;
import com.spare.house.job.CurrentHouseJob;
import com.spare.house.job.EstateAnalyseJob;
import com.spare.house.util.SparkHouseConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by dada on 2017/8/6.
 */
@Component
public class JobScheudler {

    @Autowired
    CurrentHouseJob currentHouseJob;

    @Autowired
    EstateAnalyseJob estateAnalyseJob;

    @Autowired
    MongoDao mongoDao;

    private void startCurrentHouseJob() {
        mongoDao.clearCollectionData(SparkHouseConstants.COLLECTION_HOUSE_CURRENT);
        currentHouseJob.start();
    }

    private void startEstateAnalyseJob() {
        estateAnalyseJob.start();
    }

    @PostConstruct
    private void start() {
//        startCurrentHouseJob();
//
        startEstateAnalyseJob();
        stop();
    }

    private void stop() {
        System.exit(0);
    }

}
