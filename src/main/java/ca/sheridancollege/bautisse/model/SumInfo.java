package ca.sheridancollege.bautisse.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumInfo implements Writable {
    public double sum;
    public int quantity;

    public SumInfo(double sum, int quantity) {
        this.sum = sum;
        this.quantity = quantity;
    }

    public double getAve() {
        return sum / (double) quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum = in.readDouble();
        this.quantity = in.readInt();
    }
}
