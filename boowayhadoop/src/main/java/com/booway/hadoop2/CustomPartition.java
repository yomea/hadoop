package com.booway.hadoop2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  自定义分区
 * Created by may on 2017/7/16.
 */
public class CustomPartition extends Partitioner<KeyPair, Text> {

    /**
     *
     * @param keyPair
     * @param text
     * @param i reducer的数量
     * @return
     */
    @Override
    public int getPartition(KeyPair keyPair, Text text, int i) {
        //根据年份进行分区，所以同一个年份将分到用一个区域
        return keyPair.getYear() / i;
    }
}
