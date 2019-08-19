package com.iflytek.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, VALUEIN ：map 阶段输出的 key 和 value
 * KEYOUT, VALUEOUT ：reducer 输出的 key 和 value 类型
 *
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // map 完了之后，到 reduce 的数据：
        // Java 1
        // Java 1
        // 累加求和：
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 将结果写出
        intWritable.set(sum);  // 这里主要是把 sum 的 int 类型转换成 IntWritable 类型
        context.write(key,intWritable);
    }
}
