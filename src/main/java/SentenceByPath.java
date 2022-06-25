import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class SentenceByPath implements WritableComparable<SentenceByPath> {

    private Text slotX;
    private Text p;
    private Text slotY;
    private DoubleWritable xAmount;
    private DoubleWritable yAmount;

    SentenceByPath(){
        this.slotX = new Text("");
        this.p = new Text("");
        this.slotY = new Text("");
        this.xAmount = new DoubleWritable(0);
        this.yAmount = new DoubleWritable(0);
    }

    public DoubleWritable getyAmount() {
        return yAmount;
    }

    public void setyAmount(double yAmount) {
        this.yAmount = new DoubleWritable(yAmount);
    }

    public DoubleWritable getxAmount() {
        return xAmount;
    }

    public void setxAmount(double xAmount) {
        this.xAmount = new DoubleWritable(xAmount);
    }

    public Text getSlotX() {
        return slotX;
    }

    public Text getP() {
        return p;
    }

    public Text getSlotY() {
        return slotY;
    }

    public static class WordData {
        int index;
        String word;
        int superiorIndex;
        String preposition;

        public WordData(int index, String word, int superiorIndex, String preposition) {
            this.index = index;
            this.word = word;
            this.superiorIndex = superiorIndex;
            this.preposition = preposition;
        }

        public boolean isNoun() {
            String[] valid_nouns = {"NN", "NNS", "NNP", "NNPS", "PRP"};
            boolean value = Arrays.asList(valid_nouns).contains(this.preposition);
            return value;
        }

        public boolean isValidVerb() {
            String[] valid_verbs = {"VB", "VBD", "VBG", "VBN", "VBP", "VBZ"};
            boolean value = Arrays.asList(valid_verbs).contains(this.preposition);
            return value;
        }
    }

    public SentenceByPath(String slotX, String p, String slotY) {
        this.p = new Text(p);
        this.slotX = new Text(slotX);
        this.slotY = new Text(slotY);
        this.xAmount = new DoubleWritable(0);
        this.yAmount = new DoubleWritable(0);
    }

    public void ReverseSentence(){
        Text temp = slotX;
        slotX = slotY;
        slotY = temp;
    }
    @Override
    public int compareTo(SentenceByPath o) {
        int ret = this.p.compareTo(o.p);
        if (ret == 0){
            ret = this.slotX.compareTo(o.slotX);
        }
        if (ret == 0){
            ret = this.slotY.compareTo(o.slotY);
        }
        return ret;
    }

    @Override
    public  String toString(){
        return String.format("%s\t%s\t%s\t%s\t%s", this.slotX, this.p, this.slotY, this.xAmount.toString(), this.yAmount.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        slotX.write(dataOutput);
        p.write(dataOutput);
        slotY.write(dataOutput);
        xAmount.write(dataOutput);
        yAmount.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        slotX.readFields(dataInput);
        p.readFields(dataInput);
        slotY.readFields(dataInput);
        xAmount.readFields(dataInput);
        yAmount.readFields(dataInput);
    }
}