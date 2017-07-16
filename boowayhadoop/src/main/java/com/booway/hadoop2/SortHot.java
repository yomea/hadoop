package com.booway.hadoop2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by may on 2017/7/16.
 */
public class SortHot extends WritableComparator{

    public SortHot() {

        super(KeyPair.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        KeyPair akeyPair = (KeyPair)a;
        KeyPair bkeyPair = (KeyPair)b;
        int res = Integer.compare(akeyPair.getYear(), bkeyPair.getYear());

        if(0 != res) {
            return res;

        }


        return Integer.compare(bkeyPair.getHot(), akeyPair.getHot());
    }
}
