package com.iflytek.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shenheping on 2019/9/2.
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        // 循环写出
        for (BytesWritable value : values) {
            context.write(key,value);
        }
    }
}
