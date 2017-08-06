package com.spare.house.job;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.spare.house.model.House;
import com.spare.house.util.ModelConveter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 以小区维度，分析小区的均价，精度到1元
 * 结果加入到estate.trend collection
 * 统计过程：
 * 1.pair(estaeName, House)
 * Created by dada on 2017/8/6.
 */

@Component
public class CurrentHouseJob extends SparkJob {

    private static Logger logger = LoggerFactory.getLogger(CurrentHouseJob.class);

    private static final long THREE_DAY_MILLIs = 3 * 86400 * 1000;

    private WriteConfig getWriteConfig(JavaSparkContext jsc) {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "house.current");
        return WriteConfig.create(jsc).withOptions(writeOverrides);
    }

    @Override
    public void start() {

        long curTs = System.currentTimeMillis();

        //1.查询时，要关注一下有没有已下架的
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("mongoConnect")
                .config("spark.mongodb.input.uri", String.format("mongodb://127.0.0.1/lianjia.house.detail"))
                .config("spark.mongodb.output.uri",  String.format("mongodb://%s:%d/%s.house.current", host, port, database))
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(sparkContext);

        //
        JavaRDD<House> houseJavaRDD = rdd.map((Document document) -> {
            House house = ModelConveter.convertToHouse(document);
//            if(! estateDateMap.containsKey(house.getEstateName())) {
//                estateDateMap.put(house.getEstateName(), house.getGmtCreated());
//            }
//            if(house.getGmtCreated().getTime() > estateDateMap.get(house.getEstateName()).getTime()) {
//                estateDateMap.put(house.getEstateName(), house.getGmtCreated());
//            }
            return house;
        });

//        logger.info("Total {}", houseJavaRDD.count());

        JavaRDD<House> filtered = houseJavaRDD.filter((House house) -> {
            if(house.getTitle() == null) {
                //过滤掉异常的
                return false;
            }
            //只根据每个小区的最新采集时间，返回三天内采集的数据
            if(Math.abs(house.getGmtCreated().getTime() - curTs) < THREE_DAY_MILLIs) {
                return true;
            } else {
                return false;
            }
        });

        logger.info("After filted {}", filtered.count());

        JavaRDD<Document> writeResults = filtered.mapToPair((House result) -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String date = sdf.format(result.getGmtCreated());
            return new Tuple2<String, House>(result.getLink(), result);
        }).groupByKey().map((result) -> {
            Iterator<House> houseIterator = result._2().iterator();
            long ts = 0;
            House newest = null;
            while (houseIterator.hasNext()) {
                House house = houseIterator.next();
                if(house.getGmtCreated().getTime() > ts) {
                    ts = house.getGmtCreated().getTime();
                    newest = house;
                }
//                logger.info("Key {}, value {} {}", result._1(), house.getLink(), house.getTitle(), house.getGmtCreated());
            }
            Document document = ModelConveter.convertHouseToDocument(newest);
            return document;
        });

        MongoSpark.save(writeResults, getWriteConfig(sparkContext));
        sparkContext.close();
//
//
//        pointToNewTable();
    }
}
