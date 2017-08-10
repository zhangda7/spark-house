package com.spare.house.job;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.spare.house.model.HouseResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.scalatest.Doc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 维度：一套房子
 * 分析一套房子的房价变化趋势
 * 数据存入house.result.2017-06-30表
 * spark的job每次执行的结果表必须是带日期后缀的
 * Created by dada on 2017/6/28.
 */
public class SignalHouseJob {

    private static Logger logger = LoggerFactory.getLogger(SignalHouseJob.class);

    public void start() {
        clearCollectionData("");

        //TODO
        //1.查询时，要关注一下有没有已下架的
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("mongoConnect")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/lianjia.house.detail")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/lianjia.house.result")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(sparkContext);

        JavaPairRDD<String, HouseResult> pairResult = rdd.map((Document house) -> {
            HouseResult result = new HouseResult();
            result.setLink(house.getString("link"));
            result.setTitle(house.getString("title"));
            result.setTrendList(new ArrayList<>());
            HouseResult.Trend trend = result.new Trend();
            Object gmtCreated = house.get("gmtCreated");
            if(gmtCreated instanceof String) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
                trend.setDate(simpleDateFormat.parse((String) gmtCreated));
            } else if(gmtCreated instanceof Date) {
                trend.setDate((Date) gmtCreated);
            }
//            trend.setDate(house.getString("gmtCreated"));
            trend.setPrice(house.getString("price"));
            result.getTrendList().add(trend);
            return result;
        }).mapToPair((HouseResult result) -> new Tuple2<>(result.getLink(), result))
        .reduceByKey((HouseResult h1, HouseResult h2) -> {
            h1.getTrendList().addAll(h2.getTrendList());
            return h1;
        });
        pairResult.foreach((result) -> logger.info("House {} Price {}", result._2().getTitle(), result._2().getTrendList()));
        JavaRDD<Document> writeResults = pairResult.values().map((houseResult) -> {
            Document document = new Document();
            document.put("title", houseResult.getTitle());
            document.put("houseLink", houseResult.getLink());
            List<Document> trends = new ArrayList<>();
            int previousPrice = 0;
            int trendSize = 0;
            if(! CollectionUtils.isEmpty(houseResult.getTrendList())) {
                for (HouseResult.Trend trend : houseResult.getTrendList()) {
                    if(trend.getPrice() == null) {
                        continue;
                    }
                    int curPrice = Integer.parseInt(trend.getPrice());
                    if(previousPrice != 0 && curPrice == previousPrice) {
                        continue;
                    }
                    trendSize++;
                    previousPrice = curPrice;
                    Document obj = new Document();
                    obj.put("date", trend.getDate());
                    obj.put("price", trend.getPrice());
                    trends.add(obj);
//                    BasicDBObject obj = new BasicDBObject();
//                    obj.put("date", trend.getDate());
//                    obj.put("price", trend.getPrice());
//                    trends.add(obj);
                }
            }
            document.put("trendSize", trendSize);
            document.put("trend", trends);
            return document;
        });
        MongoSpark.save(writeResults, getWriteConfig(sparkContext));
        sparkContext.close();


        pointToNewTable();
    }

    private void clearCollectionData(String tableName) {

    }

    private void pointToNewTable() {

    }

    private WriteConfig getWriteConfig(JavaSparkContext jsc) {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "house.result." + sdf.format(new Date()));
        return WriteConfig.create(jsc).withOptions(writeOverrides);
    }

    public static void main(String[] args) {
        SignalHouseJob sparkMongoTest = new SignalHouseJob();
        sparkMongoTest.start();
    }

}
