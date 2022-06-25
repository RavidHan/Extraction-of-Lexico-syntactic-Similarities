import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MapReducer1 {
    public static class Mapper1
            extends Mapper<Object, Text, Sentence, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(value.toString().length() <= 1) // Sometimes the input is just byte 0.
                return;
            StringTokenizer st = new StringTokenizer(value.toString());
            int count = 0;
            LinkedList<Sentence.WordData> wordArray = new LinkedList<>();
            String temp = "";
            List<String> tempData;
            String head = "";
            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                temp = temp.replaceAll("[\\0000]", "");
                tempData = Arrays.asList(temp.split("/"));

                if (head.isEmpty()) {
                    head = temp;
                } else if (tempData.size() == 1) {
                    try {
                        count = Integer.parseInt(temp);
                        break;
                    }
                    catch(Exception e){
                        System.out.println(temp + " cannot be parsed as int");
                        return;
                    }
                } else if (tempData.size() != 4) { // Sometimes we get something like "//NNP/dep/3" which causes breakages
                    System.out.println(temp + " is not a valid data");
                    return;
                } else {
                    // breaking a word sequence
                    Sentence.WordData wordData = new Sentence.WordData(wordArray.size()+1, tempData.get(0), Integer.parseInt(tempData.get(3)), tempData.get(1));
                    if (wordData.superiorIndex == 0 && !wordData.isValidVerb()) {
                        return;
                    }
                    wordArray.add(wordData);


                }
            }
            if (!wordArray.getFirst().isNoun() || !wordArray.getLast().isNoun()) {
                System.out.println("SlotX: " + wordArray.getFirst().word + " or SlotY: " + wordArray.getLast().word + ": head is not in a good preposition");
                return;
            }
            StringBuilder body = new StringBuilder();
            for (Sentence.WordData word:
                    wordArray.subList(1, wordArray.size()-1)) {
                body.append(word.word).append(" ");
            }
            Sentence sentence = new Sentence(wordArray.getFirst().word, body.substring(0, body.length()-1), wordArray.getLast().word);
            context.write(sentence, new DoubleWritable(count));
            context.write(new Sentence(sentence.getSlotX().toString(), "*", "*"), new DoubleWritable(count));
            context.write(new Sentence("*", "*", "*"), new DoubleWritable(count));
        }
    }

    public static class Reducer1
            extends Reducer<Sentence, DoubleWritable, Sentence, DoubleWritable> {
        private double xCount = -1;
        private String slotX = "";
        private Text star = new Text("*");
        private DoubleWritable writable_sum = new DoubleWritable();

        public void reduce(Sentence key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

           double sum = 0;

            if (key.getSlotX().equals(star) && key.getSlotY().equals(star)){
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
                writable_sum.set(sum);
                context.write(key, writable_sum);
                return;
            }
            if (!slotX.equals(key.getSlotX()) && key.getSlotY().equals(star)){
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
                xCount = sum;
                return;
            }

            for (DoubleWritable val: values) {
                sum += val.get();
            }
            writable_sum.set(sum);
            key.setxAmount(new DoubleWritable(xCount));
            context.write(key, writable_sum);


        }
    }

    public static class SlotXPartitioner extends Partitioner<Sentence, DoubleWritable> {
        @Override
        public int getPartition(Sentence sentence, DoubleWritable doubleWritable, int i) {
            return sentence.getSlotX().hashCode() % i;
        }
    }
}
