package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        IntWritable treeID = new IntWritable();
        IntWritable age = new IntWritable();

        String[] cols = value.toString().split(";");
        if (cols[6].equals("HAUTEUR"))
            return;

        try{
            treeID.set(Integer.parseInt(cols[11]));
            age.set(Integer.parseInt(cols[5]));
        }
        catch (NumberFormatException ex){
            return;
        }

        context.write(treeID, age);
    }
}
