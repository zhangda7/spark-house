package com.spare.house.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by dada on 2017/6/30.
 * House Trend
 */
public class HouseTrend implements Serializable {

    private long serialVersionUID = 1L;

    private String title;

    private String link;

    private List<Trend> trendList;

    public class Trend implements Serializable {
        private long serialVersionUID = 1L;

        private String date;

        private String price;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getPrice() {
            return price;
        }

        public void setPrice(String price) {
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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public List<Trend> getTrendList() {
        return trendList;
    }

    public void setTrendList(List<Trend> trendList) {
        this.trendList = trendList;
    }
}
