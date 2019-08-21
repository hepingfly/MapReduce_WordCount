package com.iflytek.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper 代码
 * 泛型第一个参数表示偏移量
 * 第二个参数表示读取一行内容
 * 第三个参数表示 Mapper 输出的时候用手机号作为 key
 * 第四个参数表示 Mapper 输出的时候用 FlowBean 作为 value
 *
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
         * 输入数据格式：
         * 7 18375438796 172.31.3.196 1116 954 200
         */
        // 1.获取一行
        String line = value.toString();  // value 就是这一行数据的内容,是 Text 类型，需要把它变成 String 类型

        // 2.使用 \t 切割这一行内容
        String[] fields = line.split("\t");

        // 3. 封装对象
        k.set(fields[1]);   // 封装手机号作为 key
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        // 4. 写出
        context.write(k,v);
    }
}
