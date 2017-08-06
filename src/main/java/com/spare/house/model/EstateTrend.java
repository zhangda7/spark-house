package com.spare.house.model;

import lombok.Data;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dada on 2017/6/30.
 * House Trend
 */
@Data
public class EstateTrend implements Serializable {

    private long serialVersionUID = 1L;

    private ObjectId _id;

    private String estateName;

    private String estateLianjiaId;

    private String estateObjectId;

    private List<Trend> trendList;

    public EstateTrend() {
        trendList = new ArrayList<>();
    }

    public void addTrend(Trend trend) {
        trendList.add(trend);
    }

    public class Trend implements Serializable {
        private long serialVersionUID = 1L;

        private String date;

        private Double price;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public Double getPrice() {
            return price;
        }

        public void setPrice(Double price) {
            this.price = price;
        }

        @Override
        public String toString() {
            return "Trend{" +
                    "date='" + date + '\'' +
                    ", price='" + price + '\'' +
                    '}';
        }
    }
}
