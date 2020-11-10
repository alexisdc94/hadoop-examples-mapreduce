package com.opstty.mapper;

        import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

        import java.io.IOException;

//***************************************************
//********************  1.8.2  **********************
//***************************************************
public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");                    //separate the columns
        if (cols[3].equals("ESPECE"))                                   //in case if it's the first row
            return;
        context.write(new Text(cols[3]), NullWritable.get());
    }
}
