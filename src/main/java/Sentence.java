import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public class Sentence implements WritableComparable<Sentence> {

    private Text slotX;
    private Text p;
    private Text slotY;
    private BasicNode tree;

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

        public boolean isNoun() {
            return true;
        }

        public boolean isValidVerb() {
            return true;
        }
    }

    public static class NotValidSentenceException extends Exception{}

    private static class BasicNode {
        WordData key;
        BasicNode left, right;

        public BasicNode(WordData key){
            this.key = key;
            this.left = this.right = null;
        }
    }

    public void setSentence(String slotX, String p, String slotY) {
        this.p = new Text(p);
        this.slotX = new Text(slotX);
        this.slotY = new Text(slotY);
    }

    public Sentence(){
    }

    private void createNode(LinkedList<WordData> wordDataArray, int i, BasicNode[] created){

            WordData temp = wordDataArray.get(i);
            // If tis node is already created
            if (created[i] != null){
                return;
            }
            created[i] = new BasicNode(temp);
            // If 'i' is root, change root pointer and continue
            if (temp.superiorIndex == 0) {
                this.tree = created[i];
                return;
            }

        // If parent is not created, then create parent first
        if (created[temp.superiorIndex] == null)
            createNode(wordDataArray, temp.superiorIndex, created);

        // Find parent pointer
        BasicNode p = created[temp.superiorIndex];

        // If this is first child of parent
        if (p.left == null)
            p.left = created[i];
        else // If second child
            p.right = created[i];
    }

    private List<WordData> createListSentenceRepFromTree() {
        List<WordData> listRep = new LinkedList<>();
        if (this.tree == null)
            return listRep;

        Stack<BasicNode> s = new Stack<BasicNode>();
        BasicNode curr = this.tree;

        // traverse the tree
        while (curr != null || s.size() > 0)
        {

            /* Reach the left most Node of the
            curr Node */
            while (curr !=  null)
            {
                /* place pointer to a tree node on
                   the stack before traversing
                  the node's left subtree */
                s.push(curr);
                curr = curr.left;
            }

            /* Current must be NULL at this point */
            curr = s.pop();

            listRep.add(curr.key);
            /* we have visited the node and its
               left subtree.  Now, it's right
               subtree's turn */
            curr = curr.right;
        }

        return listRep;
    }

    static Sentence analyze(LinkedList<WordData> wordDataArray) throws NotValidSentenceException{
        int len = wordDataArray.size();
        BasicNode[] created = new BasicNode[len];
        Sentence sentence = new Sentence();

        // Creating tree hierarchy of the word array
        for (int i=0; i< len; i++)
            created[i] = null;

        for (int i=0; i < len; i++) {
            sentence.createNode(wordDataArray, i, created);
            if (wordDataArray.get(i).superiorIndex == 0 && !wordDataArray.get(i).isValidVerb()) {
                throw new NotValidSentenceException();
            }
        }

        List<WordData> treeSentenceRep = sentence.createListSentenceRepFromTree();
        if (treeSentenceRep.get(0).isNoun() && treeSentenceRep.get(treeSentenceRep.size()-1).isNoun()){
            sentence.slotX = new Text(treeSentenceRep.get(0).word);
            sentence.slotY = new Text(treeSentenceRep.get(treeSentenceRep.size()-1).word);
            String body = "";
            for (WordData word:
                    treeSentenceRep.subList(1, treeSentenceRep.size()-1)) {
                body += word.word + " ";
            }
            sentence.p = new Text(body.substring(0, body.length()-1));
            return sentence;
        }

        throw new NotValidSentenceException();
    }

    @Override
    public int compareTo(Sentence o) {
        int ret = this.slotX.compareTo(o.slotX);
        if (ret == 0){
            ret = this.p.compareTo(o.p);
        }
        if (ret == 0){
            ret = this.slotY.compareTo(o.slotY);
        }
        return ret;
    }

    @Override
    public  String toString(){
        return String.format("%s\t%s\t%s", this.slotX, this.p, this.slotY);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        slotX.write(dataOutput);
        p.write(dataOutput);
        slotY.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        slotX.readFields(dataInput);
        p.readFields(dataInput);
        slotY.readFields(dataInput);
    }
}
