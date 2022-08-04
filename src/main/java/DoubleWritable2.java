import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleWritable2 implements WritableComparable<DoubleWritable2> {

    public DoubleWritable2() {
        this.sumOfSlotX_Filler = new DoubleWritable(0);
        this.sumOfPath = new DoubleWritable(0);
    }

    public DoubleWritable2(double slotXFiller, double path){
        this.sumOfSlotX_Filler = new DoubleWritable(slotXFiller);
        this.sumOfPath = new DoubleWritable(path);
    }

    public DoubleWritable getSumOfSlotX_Filler() {
        return sumOfSlotX_Filler;
    }

    public void setSumOfSlotX_Filler(DoubleWritable sumOfSlotX_Filler) {
        this.sumOfSlotX_Filler = sumOfSlotX_Filler;
    }

    private DoubleWritable sumOfSlotX_Filler;
    private DoubleWritable sumOfPath;

    public DoubleWritable2(DoubleWritable sumOfSlotX_filler, DoubleWritable sumOfPath) {
        sumOfSlotX_Filler = sumOfSlotX_filler;
        this.sumOfPath = sumOfPath;
    }

    @Override
    public String toString() {
        	return String.format("%.2f,%.2f", sumOfSlotX_Filler.get(), sumOfPath.get());
    }

    @Override
    public int compareTo(DoubleWritable2 o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sumOfSlotX_Filler.write(dataOutput);
        sumOfPath.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumOfSlotX_Filler.readFields(dataInput);
        sumOfPath.readFields(dataInput);
    }

    public DoubleWritable getSumOfPath() {
        return sumOfPath;
    }

    public void setSumOfPath(DoubleWritable sumOfPath) {
        this.sumOfPath = sumOfPath;
    }

}
