package com.booway.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by may on 2017/7/15.
 *
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *
 * KEYIN map输出的到reducer中的key， VALUEIN ap输出的到reducer中的value
 * KEYOUT reducer处理后输出的key类型， VALUEOUT reducer处理后输出的value类型
 */
public class CustomReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable i : values) {
            sum += i.get();

        }
        //输出
        context.write(key,new IntWritable(sum));

    }
}
