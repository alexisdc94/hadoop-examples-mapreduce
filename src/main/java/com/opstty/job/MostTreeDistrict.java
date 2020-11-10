package com.opstty.job;

import com.opstty.mapper.TreesInDistrictMapper;
import com.opstty.reducer.TreesInDistrictReducer;
import com.opstty.mapper.MostTreesDistrictMapper;
import com.opstty.reducer.MostTreesDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opstty.myClass.DistrictAndNumber;
import java.io.File;

public class MostTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job jobi = Job.getInstance(conf, "MostTreeDistrictPhase1");
        jobi.setJarByClass(MostTreeDistrict.class);
        jobi.setMapperClass(TreesInDistrictMapper.class);
        jobi.setCombinerClass(TreesInDistrictReducer.class);

        jobi.setMapOutputKeyClass(IntWritable.class);
        jobi.setMapOutputValueClass(IntWritable.class);

        jobi.setReducerClass(TreesInDistrictReducer.class);
        jobi.setOutputKeyClass(IntWritable.class);
        jobi.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobi, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobi, new Path(args[1]));

        jobi.waitForCompletion(true);        //now we having fun


        Job joba = Job.getInstance(conf, "MostTreeDistrictPhase2");
        joba.setJarByClass(MostTreeDistrict.class);
        joba.setMapperClass(MostTreesDistrictMapper.class);
        joba.setCombinerClass(MostTreesDistrictReducer.class);
        joba.setReducerClass(MostTreesDistrictReducer.class);

        joba.setMapOutputKeyClass(NullWritable.class);
        joba.setMapOutputValueClass(DistrictAndNumber.class);

        joba.setOutputKeyClass(NullWritable.class);
        joba.setOutputValueClass(DistrictAndNumber.class);

        String path1 = args[1] + "/part-r-00000";
        String path2 = args[1] + "/results";

        FileInputFormat.addInputPath(joba, new Path(path1));
        FileOutputFormat.setOutputPath(joba, new Path(path2));

        joba.waitForCompletion(true);

        File file = new File(path1);

        if(file.delete())
        {
            System.out.println("File deleted successfully");
        }
        else
        {
            System.out.println("Failed to delete the file");
        }


        return;
    }
}
