import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;  // Import the File class
import java.io.FileWriter;
import java.io.IOException;  // Import the IOException class to handle errors

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class MapReducer3 {

    public static class Mapper3
            extends Mapper<Object, Text, FinalSentence, SlotMaps> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            int index = 0;
            int sum = 0;
            StringTokenizer st = new StringTokenizer(value.toString(), "\t,");
            FinalSentence sentence = new FinalSentence();
            SlotMaps maps = new SlotMaps();
            String firstFiller = "";
            String secondFiller = "";
            double firstFillerSum = 0.;
            double secondFillerSum = 0.;
            double slotXSum = 0.;
            double slotYSum = 0.;
            double pathSum = 0.;
            while (st.hasMoreTokens()) {
                String token = st.nextToken().replaceAll("[\\0000]", "");
                switch(index){
                    case 0:
                        firstFiller = token;
                        break;
                    case 1:
                        sentence.setSlotX(new Text(token));
                        break;
                    case 2:
                        sentence.setPath(new Text(token));
                        break;
                    case 3:
                        sentence.setSlotY(new Text(token));
                        break;
                    case 4:
                        secondFiller = token;
                        break;
                    case 5:
                        slotXSum = Double.parseDouble(token);
                        break;
                    case 6:
                        firstFillerSum = Double.parseDouble(token);
                        break;
                    case 7:
                        slotYSum = Double.parseDouble(token);
                        break;
                    case 8:
                        secondFillerSum = Double.parseDouble(token);
                        break;
                    case 9:
                        pathSum = Double.parseDouble(token);
                        break;
                }
                index++;
            }
            maps.addToSlotX(firstFiller, pathSum); // Amount of the first filler in this path
            maps.addToSlotX(firstFiller + "_SLOTX", firstFillerSum); // Amount of the first filler in all sentences
            maps.addToSlotX("*", slotXSum); // Amount of slotX in all sentences
            maps.addToSlotY(secondFiller, pathSum); // Amount of the first filler in this path
            maps.addToSlotY(secondFiller + "_SLOTY", secondFillerSum); // Amount of the second filler in all sentences
            maps.addToSlotY("*", slotYSum); // Amount of slotY in all sentences
            context.write(sentence, maps);
        }
    }

    public static class Reducer3
            extends Reducer<FinalSentence, SlotMaps, FinalSentence, SlotMaps> {
        class S3Helper{
            S3Client s3;
            String bucketName;
            public S3Helper(String bucketName){
                Region region = Region.US_WEST_2;
                s3 = S3Client.builder()
                        .region(region)
                        .build();
                this.bucketName = bucketName;
            }
            public void writeToS3(JSONObject obj, String name){
                try {
                    PutObjectRequest putObjectRequest = PutObjectRequest
                            .builder()
                            .bucket(bucketName)
                            .key("results/" + name)
                            .build();
                    s3.putObject(putObjectRequest,
                            RequestBody.fromBytes(obj.toString().getBytes(StandardCharsets.UTF_8)));
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
        }

        private S3Helper s3helper;
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            s3helper = new S3Helper("liordia321");
        }

        public void reduce(FinalSentence key, Iterable<SlotMaps> values,
                           Context context
        ) throws IOException, InterruptedException {
            String firstFillerStr = "";
            String secondFillerStr = "";
            double firstFiller = 0.;
            double secondFiller = 0.;
            double slotXSum = 0.;
            double slotYSum = 0.;
            double path = 0.;
            double p = 0.;
            double accumulated_path = 0.;
            HashMap<String, Double> aggrSlotX = new HashMap<>();
            HashMap<String, Double> aggrSlotY = new HashMap<>();
            HashMap<String, Double> aggrSlotXTotal = new HashMap<>();
            HashMap<String, Double> aggrSlotYTotal = new HashMap<>();

            MapWritable slotXMap, slotYMap;
            for(SlotMaps maps : values){
                slotXMap = maps.getSlotXMap();
                slotYMap = maps.getSlotYMap();
                for(Writable v : slotXMap.keySet()){
                    Text tempText = (Text)v;
                    String tempString = tempText.toString();
                    if(tempString.contains("*"))
                        slotXSum = ((DoubleWritable)slotXMap.get(tempText)).get();
                    else if(tempString.contains("_SLOTX"))
                        firstFiller = ((DoubleWritable)slotXMap.get(tempText)).get();
                    else {
                        firstFillerStr = tempString;
                        path = ((DoubleWritable) slotXMap.get(tempText)).get();
                    }
                }
                for(Writable v : slotYMap.keySet()){
                    Text tempText = (Text)v;
                    String tempString = tempText.toString();
                    if(tempString.contains("*"))
                        slotYSum = ((DoubleWritable)slotYMap.get(tempText)).get();
                    else if(tempString.contains("_SLOTY"))
                        secondFiller = ((DoubleWritable)slotYMap.get(tempText)).get();
                    else {
                        secondFillerStr = tempString;
                        p = ((DoubleWritable) slotYMap.get(tempText)).get();
                        if (p != path)
                            System.out.println("Path from Y is different to path from X");
                    }
                }
                accumulated_path += path;

                aggrSlotX.put(firstFillerStr, aggrSlotX.getOrDefault(firstFillerStr, 0.) + path);
                aggrSlotY.put(secondFillerStr, aggrSlotY.getOrDefault(secondFillerStr, 0.) + path);
                aggrSlotXTotal.put(firstFillerStr, aggrSlotXTotal.getOrDefault(firstFillerStr, 0.) + firstFiller);
                aggrSlotYTotal.put(secondFillerStr, aggrSlotYTotal.getOrDefault(secondFillerStr, 0.) + secondFiller);
            }

            HashMap<String, Double> slotXFeatures = calculateFeatures(aggrSlotX, aggrSlotXTotal, slotXSum, accumulated_path);
            HashMap<String, Double> slotYFeatures = calculateFeatures(aggrSlotY, aggrSlotYTotal, slotYSum, accumulated_path);

            HashMap<String, HashMap<String, Double>> bothFeatures = new HashMap<>();
            bothFeatures.put("SlotX", slotXFeatures);
            bothFeatures.put("SlotY", slotYFeatures);

            // Now we just need to upload the features to S3
            JSONObject obj = new JSONObject(bothFeatures);
            String keyName = "output/" + key.toString().replace(',','_') + ".json";
            s3helper.writeToS3(obj, keyName);
        }
    }

    public static HashMap<String, Double> calculateFeatures(HashMap<String, Double> aggrSlot, HashMap<String, Double> aggrSlotTotal, double slotSum, double acc_path){
        HashMap<String, Double> map = new HashMap<>();
        for(String s : aggrSlot.keySet()){
            double p_slot_w = aggrSlot.get(s);
            double star_slot_w = aggrSlotTotal.get(s);
            double star_slot_star = slotSum;
            double p_slot_star = acc_path / 2;
            double num = Math.log(p_slot_w) + Math.log(star_slot_star);
            double den = Math.log(p_slot_star) + Math.log(star_slot_w);
            map.put(s, num - den);
        }
        return map;
    }

    public static class FinalPartitioner extends Partitioner<FinalSentence, SlotMaps> {
        @Override
        public int getPartition(FinalSentence finalSentence, SlotMaps maps, int i) {
            return finalSentence.getSlotX().hashCode() % i;
        }
    }



}
