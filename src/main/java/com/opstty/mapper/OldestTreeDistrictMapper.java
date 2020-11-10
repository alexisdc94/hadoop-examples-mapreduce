package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opstty.myClass.DistrictAndOld;

import java.io.IOException;

//***************************************************
//********************  1.8.6  **********************
//***************************************************
public class OldestTreeDistrictMapper extends Mapper<Object, Text, NullWritable, DistrictAndOld> {
    private NullWritable lowKeyVariable = NullWritable.get();

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {


        DistrictAndOld infos = new DistrictAndOld();
        IntWritable district = new IntWritable();
        IntWritable age = new IntWritable();

        String[] cols = value.toString().split(";");
        if (cols[6].equals("HAUTEUR"))
            return;

        try{
            district.set(Integer.parseInt(cols[1]));
            age.set(Integer.parseInt(cols[5]));
        }
        catch (NumberFormatException ex){
            return;
        }

        infos.year = age.get();
        infos.district = district.get();

        context.write(lowKeyVariable, infos);
    }
}
