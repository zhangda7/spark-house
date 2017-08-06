package com.spare.house.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by dada on 2017/7/16.
 */
@Data
public class Estate implements Serializable {
    private long serialVersionUID = 1L;

    private String name;

    private String address;

    private String district;

    private String lianjiaId;

    private String link;

    private String avgPrice;
}
