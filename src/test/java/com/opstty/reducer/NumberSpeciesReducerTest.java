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
//********************  1.8.3  **********************
//***************************************************
@RunWith(MockitoJUnitRunner.class)
public class NumberSpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private NumberSpeciesReducer nSpeciestReducer;

    @Before
    public void setup() {
        this.nSpeciestReducer = new NumberSpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "Pinus";
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.nSpeciestReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),new IntWritable(3));
    }
}
