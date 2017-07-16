package com.booway.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by may on 2017/7/15.
 */
public class JobRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "s0:9001");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(JobRunner.class);
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/usr/hadoop/temp/"));
        FileOutputFormat.setOutputPath(job, new Path("/usr/hadoop/log/"));
        //等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
