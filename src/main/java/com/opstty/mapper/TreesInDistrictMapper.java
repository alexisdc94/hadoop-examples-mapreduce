package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//***************************************************
//********************  1.8.7  **********************
//***************************************************
public class TreesInDistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        IntWritable district = new IntWritable();

        String[] cols = value.toString().split(";");
        if (cols[6].equals("HAUTEUR"))
            return;

        try{
            district.set(Integer.parseInt(cols[1]));
        }
        catch (NumberFormatException ex){
            return;
        }

        context.write(district, one);
    }
}
