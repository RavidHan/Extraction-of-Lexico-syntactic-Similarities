import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleWritable5 implements WritableComparable<DoubleWritable5> {

    public DoubleWritable5() {
        this.sumOfSlotX = new DoubleWritable(0);
        this.sumOfSlotX_Filler = new DoubleWritable(0);
        this.sumOfPath = new DoubleWritable(0);
        this.sumOfSlotY = new DoubleWritable(0);
        this.sumOfSlotY_Filler = new DoubleWritable(0);
    }

    public DoubleWritable getSumOfSlotX() {
        return sumOfSlotX;
    }

    public void setSumOfSlotX(DoubleWritable sumOfSlotX) {
        this.sumOfSlotX = sumOfSlotX;
    }

    public DoubleWritable getSumOfSlotX_Filler() {
        return sumOfSlotX_Filler;
    }

    public void setSumOfSlotX_Filler(DoubleWritable sumOfSlotX_Filler) {
        this.sumOfSlotX_Filler = sumOfSlotX_Filler;
    }

    public DoubleWritable getSumOfSlotY() {
        return sumOfSlotY;
    }

    public void setSumOfSlotY(DoubleWritable sumOfSlotY) {
        this.sumOfSlotY = sumOfSlotY;
    }

    public DoubleWritable getSumOfSlotY_Filler() {
        return sumOfSlotY_Filler;
    }

    public void setSumOfSlotY_Filler(DoubleWritable sumOfSlotY_Filler) {
        this.sumOfSlotY_Filler = sumOfSlotY_Filler;
    }

    private DoubleWritable sumOfSlotX;
    private DoubleWritable sumOfSlotX_Filler;
    private DoubleWritable sumOfSlotY;
    private DoubleWritable sumOfSlotY_Filler;
    private DoubleWritable sumOfPath;

    public DoubleWritable5(DoubleWritable sumOfSlotX, DoubleWritable sumOfSlotX_filler, DoubleWritable sumOfSlotY, DoubleWritable sumOfSlotY_filler, DoubleWritable sumOfPath) {
        this.sumOfSlotX = sumOfSlotX;
        sumOfSlotX_Filler = sumOfSlotX_filler;
        this.sumOfSlotY = sumOfSlotY;
        sumOfSlotY_Filler = sumOfSlotY_filler;
        this.sumOfPath = sumOfPath;
    }

    @Override
    public String toString() {
        	return String.format("%,.2f,%,.2f,%,.2f,%,.2f,%,.2f", sumOfSlotX, sumOfSlotX_Filler, sumOfSlotY, sumOfSlotY_Filler, sumOfPath);
    }

    @Override
    public int compareTo(DoubleWritable5 o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sumOfSlotX.write(dataOutput);
        sumOfSlotX_Filler.write(dataOutput);
        sumOfSlotY.write(dataOutput);
        sumOfSlotY_Filler.write(dataOutput);
        sumOfPath.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumOfSlotX.readFields(dataInput);
        sumOfSlotX_Filler.readFields(dataInput);
        sumOfSlotY.readFields(dataInput);
        sumOfSlotY_Filler.readFields(dataInput);
        sumOfPath.readFields(dataInput);
    }

    public DoubleWritable getSumOfPath() {
        return sumOfPath;
    }

    public void setSumOfPath(DoubleWritable sumOfPath) {
        this.sumOfPath = sumOfPath;
    }

}
