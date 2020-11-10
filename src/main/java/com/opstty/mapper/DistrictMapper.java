package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");                    //separate the columns
        if (cols[1].equals("ARRONDISSEMENT"))                           //in case if it's the first row
            return;
        context.write(new Text(cols[1]), NullWritable.get());
    }
}
