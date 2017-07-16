package com.booway.hadoop2;

import com.booway.hadoop.CustomMapper;
import com.booway.hadoop.CustomReducer;
import com.booway.hadoop.JobRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by may on 2017/7/16.
 */
public class JobRun {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static class CustomMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();

            String[] sa = str.split("\t");

            if(sa.length == 2) {
                try {

                    Date date = sdf.parse(sa[0]);

                    Calendar c = Calendar.getInstance();

                    c.setTime(date);

                    int year = c.get(Calendar.YEAR);

                    int hot = Integer.valueOf(sa[1].substring(0,sa[1].indexOf("℃")));

                    context.write(new KeyPair(year, hot), value);
                } catch(Exception e) {

                    e.printStackTrace();
                }

            }

        }
    }

    public static class CustomReducer extends Reducer<KeyPair, Text, KeyPair, Text> {

        @Override
        protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t : values) {
                context.write(key, t);

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "test");
        job.setJarByClass(JobRun.class);
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(KeyPair.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(SortHot.class);
        job.setPartitionerClass(CustomPartition.class);
        job.setGroupingComparatorClass(GroupHot.class);
        FileInputFormat.addInputPath(job, new Path("/usr/hadoop/temp/"));
        FileOutputFormat.setOutputPath(job, new Path("/usr/hadoop/log/"));
        //等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
