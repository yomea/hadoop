package com.booway.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by may on 2017/7/15.
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *
 * KEYIN 输入的Key，表示输入map的这个片段在文件中的下标，VALUEIN 指的就是这个片段的内容
 * KEYOUT map输出的key类型， VALUEOUT map输出的value类型
 */
public class CustomMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        StringTokenizer st = new StringTokenizer(value.toString(), " ");//以空格进行分割
        while(st.hasMoreTokens()) {

            String str = st.nextToken();
            try {
                context.write(new Text(str), new IntWritable(1));


            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
