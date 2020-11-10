package com.opstty.job;

import com.opstty.mapper.OldestTreeDistrictMapper;
import com.opstty.reducer.OldestTreeDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opstty.myClass.DistrictAndOld;

//***************************************************
//********************  1.8.6  **********************
//***************************************************
public class OldestTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "oldestanddistrict");
        job.setJarByClass(OldestTreeDistrict.class);
        job.setMapperClass(OldestTreeDistrictMapper.class);
        job.setCombinerClass(OldestTreeDistrictReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DistrictAndOld.class);

        job.setReducerClass(OldestTreeDistrictReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
