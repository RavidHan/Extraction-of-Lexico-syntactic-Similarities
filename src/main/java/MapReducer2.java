import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer2 {

    public static class Mapper1
            extends Mapper<Object, Text, SentenceTwo, DoubleWritable5> {

        // automobile,nsubj,start from,rest,pobj	10,5,3

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(value.toString().length() <= 1) // Sometimes the input is just byte 0.
                return;

            int index = 0;
            int sum = 0;
            StringTokenizer st = new StringTokenizer(value.toString(), "\t,");
            SentenceTwo sentenceTwo = new SentenceTwo();
            DoubleWritable5 doubleWritable5 = new DoubleWritable5();

            while(st.hasMoreTokens()) {
                String token = st.nextToken().replaceAll("[\\0000]", "");;
                if (index == 0){
                    sentenceTwo.setFirstFiller(new Text(token));
                } else if (index == 1) {
                    sentenceTwo.setSlotX(new Text(token));
                } else if (index == 2) {
                    sentenceTwo.setPath(new Text(token));
                } else if (index == 3) {
                    sentenceTwo.setSlotY(new Text(token));
                } else if (index == 4){
                    sentenceTwo.setSecondFiller(new Text(token));
                } else if (index == 5){
                    try {
                        doubleWritable5.setSumOfSlotX(new DoubleWritable(Double.parseDouble(token)));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                } else if (index == 6){
                    try {
                        doubleWritable5.setSumOfSlotX_Filler(new DoubleWritable(Double.parseDouble(token)));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                } else if (index == 7){
                    try {
                        doubleWritable5.setSumOfPath(new DoubleWritable(Double.parseDouble(token)));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    break;
                }
                index++;
            }

            doubleWritable5 = sentenceTwo.adjustToReduce(doubleWritable5);
            context.write(sentenceTwo, doubleWritable5);
        }
    }

    public static class Combiner extends Reducer<SentenceTwo, DoubleWritable5, SentenceTwo,DoubleWritable5> {
        public void reduce(SentenceTwo key, Iterable<DoubleWritable5> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            DoubleWritable5 doubleWritable5 = new DoubleWritable5();
            if(key.getFirstFiller().equals("*")) {
                for (DoubleWritable5 val : values)
                    sum += val.getSumOfSlotX().get();

                doubleWritable5.setSumOfSlotX(new DoubleWritable(sum));
                context.write(key, doubleWritable5);
                return;
            }

            if(key.getPath().equals("*")){
                for(DoubleWritable5 val : values)
                    sum += val.getSumOfSlotX_Filler().get();
                doubleWritable5.setSumOfSlotX_Filler(new DoubleWritable(sum));
                context.write(key, doubleWritable5);
                return;
            }

            for(DoubleWritable5 val : values) {
                sum += val.getSumOfPath().get();
            }
            doubleWritable5.setSumOfPath(new DoubleWritable(sum));
            context.write(key, doubleWritable5);
        }
    }

    public static class Reducer2
            extends Reducer<SentenceTwo, DoubleWritable5, SentenceTwo, DoubleWritable5> {
        private double slotX_sum = 0.;
        private double fillerX_sum = 0.;
        private String slotX = "";
        private String fillerX = "";


        public void reduce(SentenceTwo key, Iterable<DoubleWritable5> values,
                           Context context
        ) throws IOException, InterruptedException {

            double sum = 0;
            DoubleWritable sumSlotY = new DoubleWritable(0);
            DoubleWritable sumSecondFiller = new DoubleWritable(0);

            if(key.getFirstFiller().equals("*")){
                slotX = key.getSlotX();
                for(DoubleWritable5 val : values)
                    sum += val.getSumOfSlotX().get();
                slotX_sum = sum;
                return;
            }

            if(key.getPath().equals("*")){
                fillerX = key.getFirstFiller();
                for(DoubleWritable5 val : values)
                    sum += val.getSumOfSlotX_Filler().get();
                fillerX_sum = sum;
                return;
            }

            for(DoubleWritable5 val : values) {
                sum += val.getSumOfPath().get();
                sumSlotY = val.getSumOfSlotY();
                sumSecondFiller = val.getSumOfSlotY_Filler();
            }

            DoubleWritable5 value = new DoubleWritable5(sumSlotY, sumSecondFiller,
                    new DoubleWritable(slotX_sum),
                    new DoubleWritable(fillerX_sum),
                    new DoubleWritable(sum));
            key.adjustToEnd();
            context.write(key, value);
        }
    }

    public static class SlotXPartitioner extends Partitioner<SentenceOne, DoubleWritable> {
        @Override
        public int getPartition(SentenceOne sentenceOne, DoubleWritable doubleWritable, int i) {
            return sentenceOne.getSlotX().hashCode() % i;
        }
    }
}
