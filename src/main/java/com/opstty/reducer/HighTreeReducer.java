package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//***************************************************
//********************  1.8.5  **********************
//***************************************************
public class HighTreeReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private IntWritable result = new IntWritable(0);

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int height = 0;

        for (Text species : values) {
            context.write(key, species);
        }

    }
}
