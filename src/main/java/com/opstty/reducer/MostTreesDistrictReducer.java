package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.opstty.myClass.DistrictAndNumber;

import java.io.IOException;

//***************************************************
//********************  1.8.7  **********************
//***************************************************
public class MostTreesDistrictReducer extends Reducer<NullWritable, DistrictAndNumber, NullWritable, DistrictAndNumber> {

    public void reduce(NullWritable key, Iterable<DistrictAndNumber> values, Context context)
            throws IOException, InterruptedException {
        DistrictAndNumber mostTrees = new DistrictAndNumber();

        int howManyTrees = 0;
        int district = -1;

        for (DistrictAndNumber val : values) {
            if(howManyTrees < val.number){
                howManyTrees = val.number;
                district = val.district;
            }

        }

        mostTrees.number = howManyTrees;
        mostTrees.district = district;

        context.write(key, mostTrees);
    }
}
