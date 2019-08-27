package com.iflytek.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入数据：
 *   shp  ni hao
     heping hi
     shp lihai
     heping happy
   期望输出数据：
     shp  2
     heping 2
 */
public class KVTextMapper extends Mapper<Text,Text,Text,IntWritable> {
    IntWritable v = new IntWritable(1); // 遍历一个 key 就把它的 value 次数计为 1次
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 我想统计每一行第一个单词相同的次数，所以我只关心第一个单词，至于 value 是什么不重要

        // 写出   shp ni hao
        context.write(key,v);   // key 就是每一行第一个单词, v 是 1
    }
}
