package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//***************************************************
//********************  1.8.3  **********************
//***************************************************
@RunWith(MockitoJUnitRunner.class)
public class NumberSpeciesMapperTest {

    String testLine ="(48.8204495642, 2.44579219199);12;Pinus;nigra;Pinaceae;1870;25.0;248.0;route de la Tourelle, route du Point de Vue;Pin noir;Austriaca;97;Bois de Vincennes (lac de gravelle)";

    @Mock
    private Mapper.Context context;
    private NumberSpeciesMapper nSpeciesMapper;

    @Before
    public void setup() {
        this.nSpeciesMapper = new NumberSpeciesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {

        this.nSpeciesMapper.map(null, new Text(testLine), this.context);
        verify(this.context, times(1))
                .write(new Text("Pinus"), new IntWritable(1));
    }
}