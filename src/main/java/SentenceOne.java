import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class SentenceOne implements WritableComparable<SentenceOne> {

    private Text firstFiller;
    private Text slotX;
    private Text path;
    private Text slotY;
    private Text secondFiller;
    private DoubleWritable sumOfSlotX;
    private DoubleWritable sumOfSlotX_Filler;

    public String getFirstFiller() {
        return firstFiller.toString();
    }

    public void setFirstFiller(Text firstFiller) {
        this.firstFiller = firstFiller;
    }

    public String getSlotX() {
        return slotX.toString();
    }

    public void setSlotX(Text slotX) {
        this.slotX = slotX;
    }

    public String getPath() {
        return path.toString();
    }

    public void setPath(Text path) {
        this.path = path;
    }

    public String getSlotY() {
        return slotY.toString();
    }

    public void setSlotY(Text slotY) {
        this.slotY = slotY;
    }

    public String  getSecondFiller() {
        return secondFiller.toString();
    }

    public void setSecondFiller(Text secondFiller) {
        this.secondFiller = secondFiller;
    }

    public Double getSumOfSlotX() {
        return sumOfSlotX.get();
    }

    public void setSumOfSlotX(DoubleWritable sumOfSlotX) {
        this.sumOfSlotX = sumOfSlotX;
    }

    public Double getSumOfSlotX_Filler() {
        return sumOfSlotX_Filler.get();
    }

    public void setSumOfSlotX_Filler(DoubleWritable sumOfSlotX_Filler) {
        this.sumOfSlotX_Filler = sumOfSlotX_Filler;
    }

    SentenceOne(){
      this.firstFiller = new Text("");
      this.slotX = new Text("");
      this.path = new Text("");
      this.slotY = new Text("");
      this.secondFiller = new Text("");
      this.sumOfSlotX = new DoubleWritable(0.);
      this.sumOfSlotX_Filler = new DoubleWritable(0.);
    }

    public SentenceOne(String firstFiller, String slotX, String path, String slotY, String secondFiller, double sumOfSlotX, double sumOfSlotX_Filler){
        this.firstFiller = new Text(firstFiller);
        this.slotX = new Text(slotX);
        this.path = new Text(path);
        this.slotY = new Text(slotY);
        this.secondFiller = new Text(secondFiller);
        this.sumOfSlotX = new DoubleWritable(sumOfSlotX);
        this.sumOfSlotX_Filler = new DoubleWritable(sumOfSlotX_Filler);
    }


    @Override
    public int compareTo(SentenceOne o) {
        int ret = getSlotX().compareTo(o.getSlotX());
        if (ret == 0){
            ret = getFirstFiller().compareTo(o.getFirstFiller());
        }
        if (ret == 0){
            ret = getPath().compareTo(o.getPath());
        }
        if (ret == 0){
            ret = getSlotY().compareTo(o.getSlotY());
        }
        if (ret == 0){
            ret = getSecondFiller().compareTo(o.getSecondFiller());
        }
        return ret;
    }

    @Override
    public  String toString(){
        return String.format("%s\t%s\t%s\t%s\t%s\t%,.2f\t%,.2f", getFirstFiller(), getSlotX(), getPath(), getSlotY(), getSecondFiller(), getSumOfSlotX(), getSumOfSlotX_Filler());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        firstFiller.write(dataOutput);
        slotX.write(dataOutput);
        path.write(dataOutput);
        slotY.write(dataOutput);
        secondFiller.write(dataOutput);
        sumOfSlotX.write(dataOutput);
        sumOfSlotX_Filler.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstFiller.readFields(dataInput);
        slotX.readFields(dataInput);
        path.readFields(dataInput);
        slotY.readFields(dataInput);
        secondFiller.readFields(dataInput);
        sumOfSlotX.readFields(dataInput);
        sumOfSlotX_Filler.readFields(dataInput);
    }
}
