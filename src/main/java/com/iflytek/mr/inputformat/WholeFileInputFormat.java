package com.iflytek.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义 InputFormat
 * 需要继承 FileInputFormat
 * 文件路径 + 名称为 key，因此对应 Text 类型
 * 整个文件作为 value，因此对应 BytesWritable 类型
 */
public class WholeFileInputFormat extends FileInputFormat<Text,BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeRecoderReader recoderReader = new WholeRecoderReader();
        recoderReader.initialize(inputSplit,taskAttemptContext);
        return recoderReader;
    }
}
