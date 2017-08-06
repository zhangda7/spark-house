package com.spare.house.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * House POJO
 * Created by dada on 2017/7/29.
 */
@Data
public class House implements Serializable {
    private long serialVersionUID = 1L;

    private long id;

    private String objectId;

    private String houseLianjiaId;

    private String title;

    private String link;

    private String estateObjectId;

    private String estateLianjiaId;

    private String estateName;

    private Date gmtCreated;

    private Double price;

    private String city;

    private String houseType;

    private String area;

    private String floor;


}
