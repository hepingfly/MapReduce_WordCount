package com.iflytek.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by shenheping on 2019/8/29.
 */
public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/shenheping/Desktop/mr","/Users/shenheping/Desktop/output"};
        Configuration configuration = new Configuration();
        // 1. 获取 job 对象
        Job job = Job.getInstance(configuration);
        // 2.设置输出的 jar 路径
        job.setJarByClass(NLineDriver.class);
        // 3.关联 Mapper 和 Reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        // 4.设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5.设置最终输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6.设置输入输出数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7. 设置每个切片 InputSplit 中划分三条记录（三行一个切片）
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        // 8. 使用 NLineInputFormat 处理记录数
        job.setInputFormatClass(NLineInputFormat.class);
        // 7.提交 job
        job.waitForCompletion(true);
    }
}
