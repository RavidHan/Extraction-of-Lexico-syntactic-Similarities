import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer2 {

    public static class Mapper2
            extends Mapper<Object, Text, SentenceOneY, DoubleWritable2> {

        // automobile,start from,rest	10,3
        private static final String star = "*";

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(value.toString().length() <= 1) // Sometimes the input is just byte 0.
                return;

            int index = 0;
            StringTokenizer st = new StringTokenizer(value.toString(), "\t,");
            SentenceOneY sentenceTwo = new SentenceOneY();
            DoubleWritable2 doubleWritable2 = new DoubleWritable2();

            while(st.hasMoreTokens()) {
                String token = st.nextToken().replaceAll("[\\0000]", "");;
                if (index == 0){
                    sentenceTwo.setFirstFiller(new Text(token));
                } else if (index == 1) {
                    sentenceTwo.setPath(new Text(token));
                } else if (index == 2) {
                    sentenceTwo.setSecondFiller(new Text(token));
                } else {
                    DoubleWritable val;
                    try {
                        val = new DoubleWritable(Double.parseDouble(token));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                    if (index == 3) {
                        if (!sentenceTwo.getSecondFiller().equals(star)) {
                            doubleWritable2.setSumOfSlotX_Filler(val);
                        }
                    } else if (index == 4) {
                        doubleWritable2.setSumOfPath(val);
                        break;
                    }
                }
                index++;
            }

            if (sentenceTwo.getFirstFiller().equals(star)){
                for (int i=0; i<20; i++) {
                    sentenceTwo.setSecondFiller(new Text(Integer.toString(i)));
                    context.write(sentenceTwo, doubleWritable2);
                }
            } else {
                if (sentenceTwo.getSecondFiller().equals(star)){
                    sentenceTwo.setSecondFiller(new Text(sentenceTwo.getFirstFiller()));
                    sentenceTwo.setFirstFiller(new Text(star));
                }
                context.write(sentenceTwo, doubleWritable2);
            }
        }
    }

    public static class Reducer2
            extends Reducer<SentenceOneY, DoubleWritable2, SentenceOneY, DoubleWritable4> {
        private DoubleWritable all_sum = new DoubleWritable(0);
        private DoubleWritable fillerY_sum = new DoubleWritable(0);


        public void reduce(SentenceOneY key, Iterable<DoubleWritable2> values,
                           Context context
        ) throws IOException, InterruptedException {

            double sum = 0;

            if (key.getPath().equals("*")){
                for (DoubleWritable2 d:
                     values)
                    all_sum = d.getSumOfPath();
                return;
            }

            if (key.getPath().equals("Y")) {
                for (DoubleWritable2 d:
                     values)
                    fillerY_sum = d.getSumOfPath();
                return;
            }

            DoubleWritable4 doubleWritable4 = new DoubleWritable4();
            doubleWritable4.setSumOfSlotY_Filler(fillerY_sum);
            doubleWritable4.setSumOfAll(all_sum);
            for (DoubleWritable2 d:
                 values) {
                doubleWritable4.setSumOfSlotX_Filler(d.getSumOfSlotX_Filler());
                doubleWritable4.setSumOfPath(d.getSumOfPath());
            }

            context.write(key, doubleWritable4);


//            if(key.getFirstFiller().equals("*")){
//                slotX = key.getSlotX();
//                for(DoubleWritable4 val : values)
//                    sum += val.getSumOfSlotX().get();
//                slotX_sum = sum;
//                return;
//            }
//
//            if(key.getPath().equals("*")){
//                fillerX = key.getFirstFiller();
//                for(DoubleWritable4 val : values)
//                    sum += val.getSumOfSlotX_Filler().get();
//                fillerX_sum = sum;
//                return;
//            }
//
//            for(DoubleWritable4 val : values) {
//                sum += val.getSumOfPath().get();
//                sumSlotY = val.getSumOfSlotY();
//                sumSecondFiller = val.getSumOfSlotY_Filler();
//            }
//
//            DoubleWritable4 value = new DoubleWritable4(sumSlotY, sumSecondFiller,
//                    new DoubleWritable(slotX_sum),
//                    new DoubleWritable(fillerX_sum),
//                    new DoubleWritable(sum));
//            key.adjustToEnd();
//            context.write(key, value);
        }
    }

    public static class SlotYPartitioner extends Partitioner<SentenceOneY, DoubleWritable2> {
        @Override
        public int getPartition(SentenceOneY sentenceTwo, DoubleWritable2 doubleWritable, int i) {
            int num;
            try {
                num = Integer.parseInt(sentenceTwo.getSecondFiller());
            } catch (Exception e) {
                num = sentenceTwo.getSecondFiller().hashCode();
            }
            return Math.abs(num % i);
        }
    }
}
