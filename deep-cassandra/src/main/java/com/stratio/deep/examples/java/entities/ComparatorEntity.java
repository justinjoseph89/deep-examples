package com.stratio.deep.examples.java.entities;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorEntity<TweetEntity> implements Comparator<com.stratio.deep.examples.java.entities.TweetEntity>,
        Serializable {

    @Override
    public int compare(com.stratio.deep.examples.java.entities.TweetEntity o1,
            com.stratio.deep.examples.java.entities.TweetEntity o2) {

        return o1.getTweetID().toString().compareTo(o2.getTweetID().toString());
    }
}
