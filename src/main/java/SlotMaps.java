import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class SlotMaps implements WritableComparable<SlotMaps> {

    private MapWritable slotXMap;
    private MapWritable slotYMap;

    SlotMaps(){
        this.slotXMap = new MapWritable();
        this.slotYMap = new MapWritable();
    }

    public MapWritable getSlotXMap(){
        return slotXMap;
    }

    public MapWritable getSlotYMap(){
        return slotYMap;
    }

    public void addToSlotX(String filler, double filler_count){
        slotXMap.put(new Text(filler), new DoubleWritable(filler_count));
    }

    public void addToSlotY(String filler, double filler_count){
        slotYMap.put(new Text(filler), new DoubleWritable(filler_count));
    }

    @Override
    public int compareTo(SlotMaps o) {
        return -1;
    }

    @Override
    public  String toString(){
        return String.format("%s,%s", slotXMap.toString(), slotYMap.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        slotXMap.write(dataOutput);
        slotYMap.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        slotXMap.readFields(dataInput);
        slotYMap.readFields(dataInput);
    }
}
