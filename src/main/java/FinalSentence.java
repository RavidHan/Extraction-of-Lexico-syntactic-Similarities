import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FinalSentence implements WritableComparable<FinalSentence> {

    private Text slotX;
    private Text path;
    private Text slotY;

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

    FinalSentence(){
        this.slotX = new Text("");
        this.path = new Text("");
        this.slotY = new Text("");
    }

    public FinalSentence(String slotX, String path, String slotY){
        this.slotX = new Text(slotX);
        this.path = new Text(path);
        this.slotY = new Text(slotY);
    }


    @Override
    public int compareTo(FinalSentence o) {
        int ret = getSlotX().compareTo(o.getSlotX());
        if (ret == 0){
            ret = getPath().compareTo(o.getPath());
        }
        if (ret == 0){
            ret = getSlotY().compareTo(o.getSlotY());
        }
        return ret;
    }

    @Override
    public  String toString(){
        return String.format("%s,%s,%s", getSlotX(), getPath(), getSlotY());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        slotX.write(dataOutput);
        path.write(dataOutput);
        slotY.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        slotX.readFields(dataInput);
        path.readFields(dataInput);
        slotY.readFields(dataInput);
    }

}
