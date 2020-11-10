package com.opstty.myClass;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAndOld implements Writable {

    public Integer year;
    public Integer district;


    public DistrictAndOld() { }

    public DistrictAndOld(int _year, int _district) {
        year = _year;
        district = _district;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(district);
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        district = in.readInt();
    }

    public static Integer read(DataInput in) throws IOException {
        DistrictAndOld w = new DistrictAndOld();
        w.readFields(in);
        return w.district;
    }

    @Override
    public String toString() {
        return "The oldest tree is in district : " + district;
    }
}