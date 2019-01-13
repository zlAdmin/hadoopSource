package com.zl.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce开发wordCount应用
 *
 * @author zhagnlei
 * @ProjectName: hadoopResource
 * @create 2018-12-08 20:48
 * @Version: 1.0
 * <p>Copyright: Copyright (zl) 2018</p>
 **/
public class WordCountApp {

    /**
     * Map:读取输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word :words) {
                //通过上下文静map结果输出
                context.write(new Text(word), one);
            }
        }
    }

    /**
     * Reduce:归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                //求key单词出现的次数总和
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 定义Driver：封装MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "wordCount");
        //设置job的处理类
        job.setJarByClass(WordCountApp.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //通过job的设置combiner,是的在map的时候统计一次，减少网络传输
        job.setCombinerClass(MyReducer.class);

        //设置Reduce相关输出路径
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业输出

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
