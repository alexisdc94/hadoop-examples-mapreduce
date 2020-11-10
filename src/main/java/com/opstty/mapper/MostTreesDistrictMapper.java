package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import com.opstty.myClass.DistrictAndNumber;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//***************************************************
//********************  1.8.7  **********************
//***************************************************
public class MostTreesDistrictMapper extends Mapper<Object, Text, NullWritable, DistrictAndNumber> {
    private NullWritable lowKey = NullWritable.get();

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        IntWritable district = new IntWritable();
        IntWritable number = new IntWritable();

        DistrictAndNumber data = new DistrictAndNumber();

        String[] cols = value.toString().split("\t");

        try{
            district.set(Integer.parseInt(cols[0]));
            number.set(Integer.parseInt(cols[1]));
        }
        catch (NumberFormatException ex){
            return;
        }

        data.district = district.get();
        data.number = number.get();

        context.write(lowKey, data);
    }
}
