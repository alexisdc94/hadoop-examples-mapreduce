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
//********************  1.8.2  **********************
//***************************************************
@RunWith(MockitoJUnitRunner.class)
public class SpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private SpeciesReducer speciestReducer;

    @Before
    public void setup() {
        this.speciestReducer = new SpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "nigra";
        IntWritable value = null;
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.speciestReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),NullWritable.get());
    }
}
