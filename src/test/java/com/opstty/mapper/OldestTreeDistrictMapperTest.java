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
//********************  1.8.6  **********************
//***************************************************

//I didn't found the way to use the DistrictAndOld class here in test section
//since my last 2 jobs use 2 custom class, i can't go further, right now with my knowlege, in testing

/*
@RunWith(MockitoJUnitRunner.class)
public class OldestTreeDistrictMapperTest {

    String testLine ="(48.8204495642, 2.44579219199);12;Pinus;nigra;Pinaceae;1870;25.0;248.0;route de la Tourelle, route du Point de Vue;Pin noir;Austriaca;97;Bois de Vincennes (lac de gravelle)";

    @Mock
    private Mapper.Context context;
    private OldestTreeDistrictMapper oTDMapper;
    private DistrictAndOld infos;

    @Before
    public void setup() {
        this.oTDMapper = new OldestTreeDistrictMapper();
        this.infos = new DistrictAndOld();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {

        this.oTDMapper.map(null, new Text(testLine), this.context);

        infos = new DistrictAndOld(1870,12);

        verify(this.context, times(1))
                .write(NullWritable.get(), infos.toString());
    }
}

 */