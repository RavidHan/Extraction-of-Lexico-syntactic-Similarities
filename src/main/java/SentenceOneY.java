import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SentenceOneY implements WritableComparable<SentenceOneY> {

    private Text firstFiller;
    private Text path;
    private Text secondFiller;

    public String getFirstFiller() {
        return firstFiller.toString();
    }

    public void setFirstFiller(Text firstFiller) {
        this.firstFiller = firstFiller;
    }

    public String getPath() {
        return path.toString();
    }

    public void setPath(Text path) {
        this.path = path;
    }

    public String  getSecondFiller() {
        return secondFiller.toString();
    }

    public void setSecondFiller(Text secondFiller) {
        this.secondFiller = secondFiller;
    }

    SentenceOneY(){
      this.firstFiller = new Text("");
      this.path = new Text("");
      this.secondFiller = new Text("");
    }

    public SentenceOneY(String firstFiller, String path, String secondFiller){
        this.firstFiller = new Text(firstFiller);
        this.path = new Text(path);
        this.secondFiller = new Text(secondFiller);
    }


    @Override
    public int compareTo(SentenceOneY o) {
        int ret = firstFiller.toString().compareTo(o.firstFiller.toString());
        if (ret == 0){
            ret = getSecondFiller().compareTo(o.getSecondFiller());
        }
        return ret;
    }

    @Override
    public  String toString(){
        return String.format("%s,%s,%s", getFirstFiller(), getPath(), getSecondFiller());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        firstFiller.write(dataOutput);
        path.write(dataOutput);
        secondFiller.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstFiller.readFields(dataInput);
        path.readFields(dataInput);
        secondFiller.readFields(dataInput);
    }
}
