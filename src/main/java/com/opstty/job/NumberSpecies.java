package com.opstty.job;

import com.opstty.mapper.NumberSpeciesMapper;
import com.opstty.reducer.NumberSpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//***************************************************
//********************  1.8.3  **********************
//***************************************************
public class NumberSpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Nspecies");
        job.setJarByClass(NumberSpecies.class);
        job.setMapperClass(NumberSpeciesMapper.class);
        job.setCombinerClass(NumberSpeciesReducer.class);
        job.setReducerClass(NumberSpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
