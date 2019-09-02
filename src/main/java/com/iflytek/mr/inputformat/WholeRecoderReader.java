package com.iflytek.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 这里继承的 RecordReader 的 kv 和你自定义的 InputFormat kv 相同
 */
public class WholeRecoderReader extends RecordReader<Text,BytesWritable> {

    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;
    /**
     * 初始化方法
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    FileSplit split;
    Configuration configuration;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 把 inputSplit 转成 FileSplit，因为我们读的就是文件
        this.split = (FileSplit) inputSplit;
        // 获取上下文的配置信息
        configuration = taskAttemptContext.getConfiguration();
    }

    /**
     * 核心业务逻辑处理方法
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            //1.获取 fileSystem 对象
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            // 2.获取输入流
            FSDataInputStream inputStream = fileSystem.open(path); // 获取到输入流后，我需要把输入流写到 BytesWritable 中
            byte[] b = new byte[(int) split.getLength()];
            /*
             * 3.将输入流拷贝到字节数组中
             * 第一个参数：输入流
             * 第二个参数：拷贝到哪个字节数组里面
             * 第三个参数：从哪个位置开始拷贝
             * 第四个参数：拷贝的长度
             */
            IOUtils.readFully(inputStream,b,0,b.length);
            // 4.封装 BytesWritable
            v.set(b,0,b.length);  // 我们的目的就是把切片的信息读到 BytesWritable 里面去
            // 5.封装 key
            k.set(path.toString());
            // 6.关闭资源
            IOUtils.closeStream(inputStream);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    /**
     * 获取当前处理的进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
