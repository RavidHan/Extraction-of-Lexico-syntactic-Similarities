import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


class WordData{
    public String word;
    public String POS;
    public String dependency;
    public int arcIndex;

    public WordData(String word, String POS, String dependency, int arcIndex){
        this.word = word;
        this.POS = POS;
        this.dependency = dependency;
        this.arcIndex = arcIndex;
    }

    boolean isValidVerb(){
        String[] valid_verbs = {"VB", "VBD", "VBG", "VBN", "VBP", "VBZ"};
        String[] auxiliary_verbs = {"does", "do", "did", "be", "am", "is", "are",
                "was", "were", "being", "been", "will", "would", "shall", "should", "may", "might", "must"};
        boolean verb = Arrays.asList(valid_verbs).contains(this.POS);
        boolean auxiliary = Arrays.asList(auxiliary_verbs).contains(this.word);
        return verb && !auxiliary;
    }

    boolean isNoun(){
        String[] valid_nouns = {"NN", "NNS", "NNP", "NNPS", "PRP"};
        boolean value = Arrays.asList(valid_nouns).contains(this.POS);
        return value;
    }

    public static WordData parseWord(String input){
        String[] splits = input.split("/");
        if(splits.length != 4)
            return null;
        String word = splits[0];
        String POS = splits[1];
        String dependency = splits[2];
        int arcIndex = Integer.parseInt(splits[3]);

        return new WordData(word, POS, dependency, arcIndex);
    }
}

public class MapReducer1 {

    public static String getPathFromWordArray(LinkedList<WordData> wordArray){
        StringBuilder sb = new StringBuilder();
        String word = wordArray.get(1).word;
        sb.append(word);
        for(int i = 2; i < wordArray.size() - 1; i++){
            word = wordArray.get(i).word;
            sb.append(" ").append(word);
        }
        return sb.toString();
    }

    public static class Mapper1
            extends Mapper<Object, Text, SentenceOneX, DoubleWritable2> {

        // start	automobile/NN/nsubj/2 start/VB/ROOT/0 from/IN/prep/2 rest/NN/pobj/3	10

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(value.toString().length() <= 1) // Sometimes the input is just byte 0.
                return;

            int index = 0;
            int sum = 0;
            StringTokenizer st = new StringTokenizer(value.toString());
            LinkedList<WordData> wordArray = new LinkedList<>();

            while(st.hasMoreTokens()) {
                String token = st.nextToken().replaceAll("[\\0000]", "");;
                WordData wordData = WordData.parseWord(token);
                if(wordData == null && index > 0) {
                    try {
                        sum = Integer.parseInt(token);
                        break;
                    } catch (Exception e) {
                        System.out.println("Error: Couldn't parse: " + token);
                        return;
                    }
                }
                if(wordData != null){
                    if(wordData.arcIndex == 0 && !wordData.isValidVerb())
                        return;
                    wordArray.add(wordData);
                }
                index++;
            }

            WordData firstWord = wordArray.getFirst();
            WordData lastWord = wordArray.getLast();
            String path = getPathFromWordArray(wordArray);

            if(firstWord.arcIndex == 0 || !firstWord.isNoun())
                return;
            if(lastWord.arcIndex == 0 || !lastWord.isNoun())
                return;

            DoubleWritable2 sums = new DoubleWritable2(0., sum);
            context.write(new SentenceOneX(firstWord.word, path, lastWord.word), sums);
            context.write(new SentenceOneX(firstWord.word, "X", "*"), sums);
            context.write(new SentenceOneX(lastWord.word, "Y", "*"), sums);
            context.write(new SentenceOneX("*", "*", "*"), sums);
        }
    }

    public static class Reducer1
            extends Reducer<SentenceOneX, DoubleWritable2, SentenceOneX, DoubleWritable2> {
        private double fillerX_sum = 0.;
        private static String fillerX = "";


        public void reduce(SentenceOneX key, Iterable<DoubleWritable2> values,
                           Context context
        ) throws IOException, InterruptedException {

            double sum = 0;
            double tempSlotX = 0;
            for(DoubleWritable2 val : values) {
                sum += val.getSumOfPath().get();
            }


            if(key.getPath().equals("X")){
                fillerX = key.getFirstFiller();
                fillerX_sum = sum;
                return;
            }
            else if(key.getPath().equals("Y") || key.getPath().equals("*")){
                context.write(key, new DoubleWritable2(0., sum));
                return;
            }
            else if (!key.getFirstFiller().equals(fillerX)){
                System.out.println("Something went wrong!");
                return;
            }

            String firstfiller = key.getFirstFiller();
            key.setFirstFiller(new Text(key.getSecondFiller()));
            key.setSecondFiller(new Text(firstfiller));
            context.write(key, new DoubleWritable2(fillerX_sum, sum));
        }
    }

    public static class SlotXPartitioner extends Partitioner<SentenceOneX, DoubleWritable2> {
        @Override
        public int getPartition(SentenceOneX sentenceOne, DoubleWritable2 doubleWritable, int i) {
            return sentenceOne.getFirstFiller().hashCode() % i;
        }
    }
}
