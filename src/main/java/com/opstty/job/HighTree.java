package com.opstty.job;

import com.opstty.mapper.HighTreeMapper;
import com.opstty.reducer.HighTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//***************************************************
//********************  1.8.5  **********************
//***************************************************
public class HighTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highest");
        job.setJarByClass(HighTree.class);
        job.setMapperClass(HighTreeMapper.class);
        job.setCombinerClass(HighTreeReducer.class);
        job.setReducerClass(HighTreeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
