import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class MapReducer2 {
    public static class Mapper2
            extends Mapper<Object, Text, Sentence, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            double slotX = -1, slotY = -1, sum = -1, temp_value = -1;
            int index = 0;
            LinkedList<String> wordArray = new LinkedList<>();
            String temp;

            boolean string_section;
            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                temp = temp.replaceAll("[\\0000]", "");

                try{    // Check if we still in the string section
                    temp_value = Double.parseDouble(temp);
                    string_section = false;
                }
                catch (Exception e){
                    string_section = true;
                }

                if(string_section) {
                    wordArray.add(temp);
                }
                else{
                    switch (index){
                        case 0:
                            slotX = temp_value;
                            break;
                        case 1:
                            slotY = temp_value;
                            break;
                        case 2:
                            sum = temp_value;
                            break;
                    } // End of switch
                    index++;
                }
            }   // End of while loop

            StringBuilder body = new StringBuilder();
            for (String word:
                    wordArray.subList(1, wordArray.size()-1)) {
                body.append(word).append(" ");
            }
            Sentence reversed_sentence = new Sentence(wordArray.getLast(), body.substring(0, body.length()-1), wordArray.getFirst());

            context.write(new Sentence(reversed_sentence.getSlotX().toString(), "*", "*"), new DoubleWritable(sum));

            reversed_sentence.setxAmount(slotX);
            context.write(reversed_sentence, new DoubleWritable(sum));



        }
    }

    public static class Reducer2
            extends Reducer<Sentence, DoubleWritable, Sentence, DoubleWritable> {
        private double yCount = -1;
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
                yCount = sum;
                return;
            }

            for (DoubleWritable val: values) {
                sum += val.get();
            }
            writable_sum.set(sum);
            key.setyAmount(yCount);
            key.ReverseSentence();
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
