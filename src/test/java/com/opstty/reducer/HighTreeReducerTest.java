package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

//***************************************************
//********************  1.8.5  **********************
//***************************************************
@RunWith(MockitoJUnitRunner.class)
public class HighTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private HighTreeReducer htReducer;

    @Before
    public void setup() {
        this.htReducer = new HighTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(25);

        String value = "Pinus";
        String value2 = "Alexus";
        String value3 = "ArbrusDumbuldarbre";


        Iterable<Text> values = Arrays.asList(new Text(value3), new Text(value2), new Text(value));
        this.htReducer.reduce(key, values, this.context);
        verify(this.context).write(new IntWritable(25), new Text("Alexus"));
    }
}
