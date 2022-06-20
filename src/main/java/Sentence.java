import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

public class Sentence implements WritableComparable<Sentence> {

    private Text slotX;
    private Text p;
    private Text slotY;

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

        public WordData() {

        }
    }

    private static class BasicNode {
        WordData key;
        BasicNode left, right;

        public BasicNode(WordData key){
            this.key = key;
            this.left = this.right = null;
        }
    }

    public Sentence(String slotX, String p, String slotY) {
        this.p = new Text(p);
        this.slotX = new Text(slotX);
        this.slotY = new Text(slotY);
    }

    private static createNode(LinkedList<WordData> wordDataArray){
        int len = wordDataArray.size();
        BasicNode root;
        BasicNode[] created = new BasicNode[len];

        // Creating tree hierarchy of the word array
        for (int i=0; i< len; i++)
            created[i] = null;

        for (int i=0; i < len; i++) {
            WordData temp = wordDataArray.get(i);
            // If tis node is already created
            if (created[i] != null){
                continue;
            }
            created[i] = new Sentence.BasicNode(temp);
            // If 'i' is root, change root pointer and continue
            if (temp.superiorIndex == 0) {
                root = created[i];
                continue;
            }

            if (created[temp.superiorIndex] == null){

            }
        }
    }

    static Sentence analyze(LinkedList<WordData> wordDataArray){

        return new Sentence("", "", "");
    }

    @Override
    public int compareTo(Sentence o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
