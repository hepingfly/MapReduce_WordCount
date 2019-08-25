package com.iflytek.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Driver 代码
 */
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取 Job 对象
        Job job = Job.getInstance(new Configuration());
        // 2. 设置 jar 路径
        job.setJarByClass(FlowCountDriver.class);
        // 3. 关联 Mapper 和 Reducer
        job.setMapperClass(FlowCountMapper.class);
        // 4. 设置 Mapper 输出的 key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 5.设置最终输出的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7.提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
