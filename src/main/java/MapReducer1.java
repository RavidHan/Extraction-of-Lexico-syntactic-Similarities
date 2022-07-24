import org.apache.hadoop.io.DoubleWritable;
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
        boolean value = Arrays.asList(valid_verbs).contains(this.POS);
        return value;
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
            extends Mapper<Object, Text, SentenceOne, DoubleWritable3> {

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
            String slotX = firstWord.dependency;
            String slotY = lastWord.dependency;
            String path = getPathFromWordArray(wordArray);

            if(firstWord.arcIndex == 0 || !firstWord.isNoun())
                return;
            if(lastWord.arcIndex == 0 || !lastWord.isNoun())
                return;

            DoubleWritable3 sums = new DoubleWritable3(0., 0., sum);
            context.write(new SentenceOne(firstWord.word, slotX, path, slotY, lastWord.word), sums);
            context.write(new SentenceOne(firstWord.word, slotX, "*", "*", "*"), sums);
            context.write(new SentenceOne(lastWord.word, slotY, "*", "*", "*"), sums);
            context.write(new SentenceOne("*", slotY, "*", "*", "*"), sums);
            context.write(new SentenceOne("*", slotX, "*", "*", "*"), sums);
        }
    }

    public static class Reducer1
            extends Reducer<SentenceOne, DoubleWritable3, SentenceOne, DoubleWritable3> {
        private double slotX_sum = 0.;
        private double fillerX_sum = 0.;
        private String slotX = "";
        private String fillerX = "";


        public void reduce(SentenceOne key, Iterable<DoubleWritable3> values,
                           Context context
        ) throws IOException, InterruptedException {

           double sum = 0;
           double tempSlotX = 0;
           double tempslotXFiller = 0;
           for(DoubleWritable3 val : values) {
               sum += val.getSumOfPath().get();
               tempSlotX = val.getSumOfSlotX().get();
               tempslotXFiller = val.getSumOfSlotX_Filler().get();
           }

           if(key.getFirstFiller().equals("*")){
               slotX = key.getSlotX();
               slotX_sum = sum;
               context.write(key, new DoubleWritable3(tempSlotX, tempslotXFiller, sum));
               return;
           }
           if(key.getPath().equals("*")){
               fillerX = key.getFirstFiller();
               fillerX_sum = sum;
               context.write(key, new DoubleWritable3(tempSlotX, tempslotXFiller, sum));
               return;
           }

           context.write(key, new DoubleWritable3(slotX_sum, fillerX_sum, sum));
        }
    }

    public static class SlotXPartitioner extends Partitioner<SentenceOne, DoubleWritable> {
        @Override
        public int getPartition(SentenceOne sentenceOne, DoubleWritable doubleWritable, int i) {
            return sentenceOne.getSlotX().hashCode() % i;
        }
    }
}
