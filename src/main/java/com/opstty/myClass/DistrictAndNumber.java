package com.opstty.myClass;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAndNumber implements Writable {

    public Integer number;
    public Integer district;


    public DistrictAndNumber() { }

    public DistrictAndNumber(int _number, int _district) {
        number = _number;
        district = _district;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(number);
        out.writeInt(district);
    }

    public void readFields(DataInput in) throws IOException {
        number = in.readInt();
        district = in.readInt();
    }

    public static Integer read(DataInput in) throws IOException {
        DistrictAndOld w = new DistrictAndOld();
        w.readFields(in);
        return w.district;
    }

    @Override
    public String toString() {
        return "The district with the most trees is : " + district;
    }
}