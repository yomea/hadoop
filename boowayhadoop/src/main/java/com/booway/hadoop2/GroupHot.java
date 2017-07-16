package com.booway.hadoop2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by may on 2017/7/16.
 */
public class GroupHot extends WritableComparator {

    public GroupHot() {

        super(KeyPair.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair akeyPair = (KeyPair)a;
        KeyPair bkeyPair = (KeyPair)b;
        return Integer.compare(akeyPair.getYear(), bkeyPair.getYear());
    }
}
