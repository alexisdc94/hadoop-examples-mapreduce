package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.opstty.myClass.DistrictAndOld;

import java.io.IOException;

//***************************************************
//********************  1.8.6  **********************
//***************************************************
public class OldestTreeDistrictReducer extends Reducer<NullWritable, DistrictAndOld, NullWritable, DistrictAndOld> {
    private IntWritable result = new IntWritable(0);

    public void reduce(NullWritable key, Iterable<DistrictAndOld> values, Context context)
            throws IOException, InterruptedException {
        DistrictAndOld younger = new DistrictAndOld(-1,0);

        int oldest = 3000;
        int district = -1;

        for (DistrictAndOld val : values) {
            if(oldest > val.year){
                oldest = val.year;
                district = val.district;
            }
        }

        younger.year = oldest;
        younger.district = district;

        context.write(key, younger);
    }
}
