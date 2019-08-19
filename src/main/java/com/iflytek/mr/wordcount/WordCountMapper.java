package com.iflytek.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 阶段
 * KEYIN 输入数据的 key 类型
 * VALUEIN 输入数据的 value 类型
 * KEYOUT 输出数据的 key 类型     java 1 html 2
 * VALUEOUT 输出数据的 value 类型
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();
    /**
     * 重写父类中的 map 方法
     * @param key  这个参数表示行的偏移量
     * @param value 这个参数表示这一行实实在在的内容
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Java Java
        // 获取一行
        String line = value.toString();
        // 根据空格去切割单词
        String[] words = line.split(" ");
        // 循环写出
        for (String word : words) {
//            Text k = new Text();     提出去，不要在 for 循环里面创建对象
            k.set(word);
//            IntWritable v = new IntWritable();
            v.set(1);
            context.write(k,v);
        }
    }
}
