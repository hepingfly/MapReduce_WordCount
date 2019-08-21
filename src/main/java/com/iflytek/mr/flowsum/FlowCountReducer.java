package com.iflytek.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer 代码
 * 泛型第一个和第二个参数是 Mapper 的输出参数
 * 第三个参数：输出的 key ：用手机号作为输出的 key
 * 第四个参数：输出的 value ：用 FlowBean 作为输出的 value
 */
public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean v = new FlowBean();
    /**
     *
     * @param key 传进来的 key 是手机号
     * @param values 传进来的 values 是 FlowBean 集合
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 手机号    上行流量 下行流量
        // 18375438907 2481 793
        // 18375438907 8976 676

        // 1.累加求和
        long sumUpflow = 0L;
        long sumDownflow = 0L;
        for (FlowBean flow : values) {
            sumUpflow += flow.getUpFlow();
            sumDownflow += flow.getDownFlow();
        }
        long sum = sumUpflow + sumDownflow;
        v.setUpFlow(sumUpflow);
        v.setDownFlow(sumDownflow);
        v.setSumFlow(sum);

        // 2.写出
        context.write(key,v);
    }
}
