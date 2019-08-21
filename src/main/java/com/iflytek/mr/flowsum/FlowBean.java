package com.iflytek.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化 bean 需要实现 Writable 接口
 */
public class FlowBean implements Writable {

    /**
     * 手机上行流量
     */
    private long upFlow;

    /**
     * 手机下行流量
     */
    private long downFlow;

    /**
     * 手机总流量
     */
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;

    }

    /**
     * 序列化方法
     * @param dataOutput
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param dataInput
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 反序列化的顺序必须和序列化的顺序一致（序列化的时候是把序列化的对象放到队列里面，先进先出，因此反序列化也要按顺序进行）
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
