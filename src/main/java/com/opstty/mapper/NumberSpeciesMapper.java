package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//***************************************************
//********************  1.8.3  **********************
//***************************************************
public class NumberSpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");                    //separate the columns
        if (cols[2].equals("GENRE"))                                   //in case if it's the first row
            return;
        context.write(new Text(cols[2]), one);
    }
}
