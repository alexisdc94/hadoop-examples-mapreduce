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
//********************  1.8.4  **********************
//***************************************************
@RunWith(MockitoJUnitRunner.class)
public class HighestTreReducerTest {
    @Mock
    private Reducer.Context context;
    private HighestTreeReducer htReducer;

    @Before
    public void setup() {
        this.htReducer = new HighestTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "Pinus";
        IntWritable value = new IntWritable(25);
        IntWritable value2 = new IntWritable(15);
        IntWritable value3 = new IntWritable(5);


        Iterable<IntWritable> values = Arrays.asList(value3, value2, value);
        this.htReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),new IntWritable(25));
    }
}
