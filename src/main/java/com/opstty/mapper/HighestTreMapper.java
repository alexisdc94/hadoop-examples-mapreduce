package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestTreMapper extends Mapper<Object, Text, Text, NullWritable> {

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        IntWritable height = new IntWritable();

        String[] cols = value.toString().split(";");
        if (cols[6].equals("HAUTEUR"))
            return;

        try{
            height.set((int) Float.parseFloat(cols[6]));
        }
        catch (NumberFormatException ex){
            height.set(0);
        }

        context.write(new Text(cols[2]), height);
    }
}
