package com.spare.house.job;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.spare.house.model.Estate;
import com.spare.house.model.EstateTrend;
import com.spare.house.model.House;
import com.spare.house.util.ModelConveter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.jws.WebParam;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dada on 2017/8/6.
 */
@Component
public class EstateAnalyseJob extends SparkJob {

    private static Logger logger = LoggerFactory.getLogger(EstateAnalyseJob.class);

    private WriteConfig getWriteConfig(JavaSparkContext jsc) {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "estate.trend");
        return WriteConfig.create(jsc).withOptions(writeOverrides);
    }

    @Override
    public void start() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("mongoConnect")
                .config("spark.mongodb.input.uri", String.format("mongodb://127.0.0.1/lianjia.house.current"))
                .config("spark.mongodb.output.uri",  String.format("mongodb://%s:%d/%s.estate.trend", host, port, database))
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<String, EstateTrend> newEstateTrendRdd = MongoSpark.load(sparkContext)
                .mapToPair((Document document) ->
                        new Tuple2<>(document.getString("estateName"), ModelConveter.convertToHouse(document)))
                .groupByKey().mapToPair((result) -> {
                    Iterator<House> iterator = result._2().iterator();
                    EstateTrend estateTrend = new EstateTrend();
                    House first = iterator.next();
                    estateTrend.setEstateName(first.getEstateName());
                    estateTrend.setEstateLianjiaId(first.getEstateLianjiaId());
//                    estateTrend.setEstateObjectId(first.getEstateObjectId());
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    String date = sdf.format(first.getGmtCreated());
                    Double sumPrice = first.getPrice();
                    double areaSum = ModelConveter.convertAreaToDouble(first.getArea());
                    while (iterator.hasNext()) {
                        House cur = iterator.next();
                        sumPrice += cur.getPrice();
                        areaSum += ModelConveter.convertAreaToDouble(cur.getArea());
                    }
                    EstateTrend.Trend trend = estateTrend.new Trend();
                    trend.setDate(date);
                    trend.setPrice(sumPrice * 10000 / areaSum);
                    estateTrend.addTrend(trend);

                    return new Tuple2<>(estateTrend.getEstateName(), estateTrend);
                });

        JavaMongoRDD<Document> metaTrendRdd = readFromEstateTrend(sparkContext);
//
        if(metaTrendRdd.count() == 0) {
//        if(System.currentTimeMillis() >1 ) {
            JavaRDD<Document> writeResults = newEstateTrendRdd.map((result) -> {
                Document document = ModelConveter.convertToDocument(result._2());
                return document;
            });
            MongoSpark.save(writeResults, getWriteConfig(sparkContext));

        } else {
            JavaPairRDD<String, Document> estateTrendRdd = metaTrendRdd
                    .mapToPair((Document document) -> new Tuple2<>(document.getString("estateName"), document));
            //
            // key : estateName
            // tuple1 : House
            // tuple2 : EstateTrend
            JavaPairRDD<String, Tuple2<Iterable<EstateTrend>, Iterable<Document>>> combinedRdd = newEstateTrendRdd.cogroup(estateTrendRdd);

            JavaRDD<Document> writeResults = combinedRdd.map((result) -> {
                if(result._2()._1() == null) {
                    //如果合并的结果，House的结果没有，直接返回原有的trend
                    return result._2()._2().iterator().next();
                }
                if(result._2()._2() == null) {
                    //如果原有的trend中没有该结果，直接返回新计算好的trend
                    return ModelConveter.convertToDocument(result._2()._1().iterator().next());
                }

                EstateTrend newTrend = result._2()._1().iterator().next();
                EstateTrend curTrend = ModelConveter.convertToEstateTrend(result._2()._2().iterator().next());
                boolean exsit = false;
                for (EstateTrend.Trend trend : curTrend.getTrendList()) {
                    if(trend.getDate().equals(newTrend.getTrendList().get(0).getDate())) {
                        exsit = true;
                        break;
                    }
                }
                if(! exsit) {
                    curTrend.addTrend(newTrend.getTrendList().get(0));
                }
                return ModelConveter.convertToDocument(curTrend);
            });

            MongoSpark.save(writeResults, getWriteConfig(sparkContext));
        }

        sparkContext.close();
    }

    private JavaMongoRDD<Document> readFromEstateTrend(JavaSparkContext jsc) {
        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "estate.trend");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // Load data using the custom ReadConfig
        JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);
        return customRdd;
    }
}
