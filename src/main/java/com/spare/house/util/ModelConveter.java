package com.spare.house.util;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.spare.house.model.Estate;
import com.spare.house.model.EstateTrend;
import com.spare.house.model.House;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by dada on 2017/7/25.
 */
public class ModelConveter {

    private static Logger logger = LoggerFactory.getLogger(ModelConveter.class);



    public static Estate convertToEstate(Document document) {
        if(document == null) {
            return null;
        }
        Estate estate = new Estate();
        estate.setName(document.getString("name"));
        estate.setDistrict(document.getString("district"));
        estate.setAddress(document.getString("address"));
        estate.setLianjiaId(document.getString("lianjiaId"));
        estate.setLink(document.getString("houseLink"));

        return estate;
    }

    public static House convertToHouse(Document document) {
        if(document == null) {
            return null;
        }
        Date gmtCreated = null;
        House house = new House();
        house.setTitle(document.getString("title"));
        house.setLink(document.getString("link"));
        house.setEstateLianjiaId(document.getString("estateLianjiaId"));
        house.setEstateName(document.getString("estateName"));
        //FIXME time convert problem
//            house.setGmtCreated(document.getDate("gmtCreated"));
        try {
            house.setPrice(Double.parseDouble(document.getString("price")));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            gmtCreated = sdf.parse(document.getString("gmtCreated"));
        } catch (NumberFormatException e) {
            logger.error("Price of house {} {} is error {}", document.getString("_id"), house.getTitle(), document.getString("price"));
        } catch (ParseException e) {
            logger.error("GmtCreated of house {} {} is error {}", document.getString("_id"), house.getTitle(), document.getString("gmtCreated"));
        } catch (NullPointerException e) {
            e.printStackTrace();
            logger.error("Attr house {} is error", document.getObjectId("_id"));
            return new House();
        }
        house.setCity(document.getString("city"));
        house.setHouseType(document.getString("houseType"));
        house.setArea(document.getString("area"));
        house.setFloor(document.getString("floor"));

        if(gmtCreated != null) {
            house.setGmtCreated(gmtCreated);
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTimeInMillis(gmtCreated.getTime());
//            calendar.set(Calendar.HOUR_OF_DAY, 0);
//            calendar.set(Calendar.MINUTE, 0);
//            calendar.set(Calendar.SECOND, 0);
//            calendar.set(Calendar.MILLISECOND, 0);
//            house.setGmtCreated(calendar.getTime());
        }

        return house;
    }

    public static Document convertHouseToDocument(House house) {
        if(house == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
        Document document = new Document();
        document.put("title", house.getTitle());
        document.put("link", house.getLink());
        document.put("estateId", house.getEstateObjectId());
        document.put("estateLianjiaId", house.getEstateLianjiaId());
        document.put("estateName", house.getEstateName());
        document.put("gmtCreated",sdf.format(house.getGmtCreated()));
        document.put("price", String.valueOf(house.getPrice()));
        document.put("city", house.getCity());
        document.put("houseType", house.getHouseType());
        document.put("area", house.getArea());
        document.put("floor", house.getFloor());

        return document;
    }

    public static List<House> convertToHouse(FindIterable<Document> iterable) {
        List<House> houseList = new ArrayList<>();
        if(iterable == null) {
            return houseList;
        }
        iterable.forEach((Block<Document>) document -> {
            House house = convertToHouse(document);
            if(house != null) {
                houseList.add(house);
            }
        });
        return houseList;
    }

    public static EstateTrend convertToEstateTrend(Document document) {
        if(document == null) {
            return null;
        }
        EstateTrend estateTrend = new EstateTrend();
        estateTrend.set_id(document.getObjectId("_id"));
        estateTrend.setEstateName(document.getString("estateName"));
        estateTrend.setEstateLianjiaId(document.getString("estateLianjiaId"));
        List<Document> trends = (List<Document>) document.get("trends");
        if(trends == null) {
            return estateTrend;
        }
        for (Document obj : trends) {
            EstateTrend.Trend trend = estateTrend.new Trend();
            trend.setDate(obj.getString("date"));
            trend.setPrice(obj.getDouble("price"));
            estateTrend.addTrend(trend);
        }
        return estateTrend;
    }

    public static Document convertToDocument(EstateTrend estateTrend) {
        if(estateTrend == null) {
            return null;
        }
        Document document = new Document();
        if(estateTrend.get_id() != null) {
            document.put("_id", estateTrend.get_id());
        }
        document.put("estateName", estateTrend.getEstateName());
        document.put("estateLianjiaId", estateTrend.getEstateLianjiaId());
        List<BasicDBObject> trends = new ArrayList<>();
        for (EstateTrend.Trend trend : estateTrend.getTrendList()) {
            BasicDBObject obj = new BasicDBObject();
            obj.put("date", trend.getDate());
            obj.put("price", trend.getPrice());
            trends.add(obj);
        }
        document.put("trends", trends);
        return document;
    }

    /**
     *
     * @param area 12.3平
     * @return 12.3
     */
    public static Double convertAreaToDouble(String area) {
        if(area == null) {
            return 0.0;
        }
        if(area.contains("平")) {
            area = area.substring(0, area.length() - 1);
        }
        return Double.parseDouble(area);
    }

}
