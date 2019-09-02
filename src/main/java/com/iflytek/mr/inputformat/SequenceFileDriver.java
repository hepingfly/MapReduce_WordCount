package com.iflytek.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by shenheping on 2019/9/2.
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/shenheping/Desktop/mr","/Users/shenheping/Desktop/output"};
        // 1.获取 job 对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2.设置 jar 包存储的位置
        job.setJarByClass(SequenceFileDriver.class);
        // 3.关联 mapper 和 reducer
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        // 4.设置 mapper 的 key vaule 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // 5.设置最终输出的 key value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        // 6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7.设置输入的 inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 8.设置输出的 outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 9.提交 job
        job.waitForCompletion(true);
    }
}
