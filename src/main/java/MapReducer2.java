import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer2 {

    public static class Mapper2
            extends Mapper<Object, Text, SentenceOneY, DoubleWritable3> {

        // automobile,start from,rest	10,3
        private static final String star = "*";

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(value.toString().length() <= 1) // Sometimes the input is just byte 0.
                return;

            int index = 0;
            StringTokenizer st = new StringTokenizer(value.toString(), "\t,");
            SentenceOneY sentenceTwo = new SentenceOneY();
            DoubleWritable3 doubleWritable3 = new DoubleWritable3();

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
                            doubleWritable3.setSumOfSlotX_Filler(val);
                        }
                    } else if (index == 4) {
                        doubleWritable3.setSumOfPath(val);
                        break;
                    }
                }
                index++;
            }
            context.write(sentenceTwo, doubleWritable3);

//            if (sentenceTwo.getFirstFiller().equals(star)){
//                for (int i=0; i<20; i++) {
//                    sentenceTwo.setSecondFiller(new Text(Integer.toString(i)));
//                    context.write(sentenceTwo, doubleWritable2);
//                }
//            } else {
//                if (sentenceTwo.getSecondFiller().equals(star)){
//                    sentenceTwo.setSecondFiller(new Text(sentenceTwo.getFirstFiller()));
//                    sentenceTwo.setFirstFiller(new Text(star));
//                }
//                context.write(sentenceTwo, doubleWritable2);
//            }
        }
    }

    public static class Reducer2
            extends Reducer<SentenceOneY, DoubleWritable3, SentenceOneY, DoubleWritable3> {
        private double fillerY_sum = 0.;


        public void reduce(SentenceOneY key, Iterable<DoubleWritable3> values,
                           Context context
        ) throws IOException, InterruptedException {

            double sum = 0;

//            if (key.getPath().equals("*")){
//                for (DoubleWritable2 d:
//                     values)
//                    all_sum = d.getSumOfPath();
//                return;
//            }

            if (key.getPath().equals("Y")) {
                for (DoubleWritable3 d:
                     values)
                    fillerY_sum = d.getSumOfPath().get();
                return;
            }

            DoubleWritable3 doubleWritable3 = new DoubleWritable3();
            doubleWritable3.setSumOfSlotY_Filler(new DoubleWritable(fillerY_sum));
            for (DoubleWritable3 d:
                 values) {
                doubleWritable3.setSumOfSlotX_Filler(d.getSumOfSlotX_Filler());
                doubleWritable3.setSumOfPath(d.getSumOfPath());
            }
            String firstFiller = key.getFirstFiller();
            key.setFirstFiller(new Text(key.getSecondFiller()));
            key.setSecondFiller(new Text(firstFiller));
            context.write(key, doubleWritable3);

        }
    }

    public static class SlotYPartitioner extends Partitioner<SentenceOneY, DoubleWritable3> {
        @Override
        public int getPartition(SentenceOneY sentenceTwo, DoubleWritable3 doubleWritable, int i) {
            int num;
            try {
                num = Integer.parseInt(sentenceTwo.getSecondFiller());
            } catch (Exception e) {
                num = sentenceTwo.getFirstFiller().hashCode();
            }
            return Math.abs(num % i);
        }
    }
}
