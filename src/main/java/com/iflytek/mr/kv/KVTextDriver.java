package com.iflytek.mr.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by shenheping on 2019/8/28.
 */
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"",""};
        Configuration configuration = new Configuration();
        // 设置切割符，用空格进行切割
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        // 1.获取 job 对象
        Job job = Job.getInstance(configuration);
        // 2.设置 jar 存储路径
        job.setJarByClass(KVTextDriver.class);
        // 3. 关联 mapper 和 reducer 类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        // 4. 设置 mapper 输出的 key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 设置最终输出 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 7. 提交 job
        job.waitForCompletion(true);
    }
}
