import Sentance.Sentance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class MapReducer1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Sentence, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int count = 0;
            LinkedList<Sentence.WordData> wordArray = new LinkedList<>();
            String temp = "";
            List<String> tempData = new LinkedList<>();
            String head = "";
            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                temp = temp.replaceAll("[\\0000]", "");
                tempData = Arrays.asList(temp.split("/"));

                if (head.isEmpty()) {
                    head = temp;
                } else if (tempData.size() == 1) {
                    count = Integer.parseInt(temp);
                    break;
                } else if (tempData.size() < 4) {
                    System.out.println(temp + " is not a valid data");
                    return;
                } else {
                    // breaking a word sequence
                    Sentence.WordData wordData = new Sentence.WordData(wordArray.size(), tempData.get(0), Integer.parseInt(tempData.get(4)), tempData.get(1));
                    if (wordData.superiorIndex == 0 && wordData.preposition != "verb") {
                        System.out.println(temp + ": head is not in a good preposition");
                        return;
                    }
                    wordArray.add(wordData);
                }
                try {
                    context.write(Sentence.analyze(wordArray), new DoubleWritable(count));
                } catch (Sentence.NotValidSentenceException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    public static class IntSumReducer
            extends Reducer<Sentence, DoubleWritable, Sentence, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private Text word = new Text();

        public void reduce(WordAndYear key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DecadePartitioner1 extends Partitioner<WordAndYear, DoubleWritable> {
        @Override
        public int getPartition(WordAndYear key, DoubleWritable value, int i) {
            return key.getDecade()/10 % i;
        }
    }
}
