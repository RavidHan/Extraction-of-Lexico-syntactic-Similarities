import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class MapReducer3 {
    public static class Mapper3
            extends Mapper<Object, Text, SentenceByPath, DoubleWritable> {

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
            SentenceByPath sentence = new SentenceByPath(wordArray.getFirst(), body.substring(0, body.length()-1), wordArray.getLast());
            sentence.setxAmount(slotX);
            sentence.setyAmount(slotY);
            context.write(sentence, new DoubleWritable(sum));



        }
    }

    public static class Reducer3
            extends Reducer<SentenceByPath, DoubleWritable, SentenceByPath, DoubleWritable> {

        private String path = "";
        private HashMap<String, Double> slotXMap = new HashMap<>();
        private HashMap<String, Double> slotYMap = new HashMap<>();
        private HashMap<String, Double> totalWordCount = new HashMap<>();
        private double totalNumber = -1;

        public void reduce(SentenceByPath key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String currentPath = key.getP().toString();
            if(path.equals(""))
                path = currentPath;

           double sum = 0;
           for(DoubleWritable val : values){
               sum += val.get();
           }

           if(!currentPath.equals(path)) { // Finished iterating over all slots in this path
                computeAndUploadList(path, slotXMap, slotYMap, totalNumber, totalWordCount);
                slotXMap = new HashMap<>();
                slotYMap = new HashMap<>();
                totalWordCount = new HashMap<>();
           }

           path = currentPath;

           String xString = key.getSlotX().toString();
           String yString = key.getSlotY().toString();
           double currentValueOfX = slotXMap.getOrDefault(xString, 0.0);
           double currentValueOfY = slotYMap.getOrDefault(yString, 0.0);
           slotXMap.put(xString, currentValueOfX + sum);
           slotYMap.put(yString, currentValueOfY + sum);
           totalWordCount.put(xString, key.getxAmount().get());
           totalWordCount.put(yString, key.getyAmount().get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            computeAndUploadList(path, slotXMap, slotYMap, totalNumber, totalWordCount);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalNumber = 1000; // TODO: get totalNumber from S3
        }
    }


    public static void computeAndUploadList(String path, HashMap<String, Double> slotXMap, HashMap<String, Double> slotYMap, double totalPaths,
                                            HashMap<String, Double> totalWordsCount){
        /*  TODO: Implement this function
            This function takes 5 arguments in order to create the final list for the current path:
            path - The path itself, for example "start"
            slotXMap - key: word, value: The total count of this word in slotX of this path
            slotYMap - key: word, value: The total count of this word in slotY of this path
            totalPaths - Total count of all paths
            totalWordsCount - key: word, value: The total count of this word in any path in any slot.
        */
        System.out.println(path);
    }


    public static class PathPartitioner extends Partitioner<SentenceByPath, DoubleWritable> {
        @Override
        public int getPartition(SentenceByPath sentence, DoubleWritable doubleWritable, int i) {
            return sentence.getP().hashCode() % i;
        }
    }
}
