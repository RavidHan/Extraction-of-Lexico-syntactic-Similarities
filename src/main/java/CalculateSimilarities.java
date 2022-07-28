import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.avro.data.Json;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class CalculateSimilarities {
    public static void main(String[] args) throws Exception {
        SentenceParts sentence1 = new SentenceParts("X start on Y");
        SentenceParts sentence2 = new SentenceParts("X start to Y");
        double leftSim = calculateSlotSim(sentence1.leftMap, sentence2.leftMap);
        double rightSim = calculateSlotSim(sentence1.rightMap, sentence2.rightMap);
        double result = Math.sqrt(leftSim * rightSim);
        System.out.println(result);
    }

    public static double calculateSlotSim(LinkedTreeMap<String, Double> first, LinkedTreeMap<String, Double> second){
        Set<String> firstSet = first.keySet();
        Set<String> intersection = new HashSet<>();
        intersection.addAll(firstSet);
        Set<String> secondSet = second.keySet();
        intersection.retainAll(secondSet);
        double num = 0.;
        double den = 0.;
        for(String word: intersection){
            num += first.get(word);
            num += second.get(word);
        }
        for(String word: firstSet){
            den += first.get(word);
        }
        for(String word: secondSet){
            den += second.get(word);
        }
        return num / den;
    }
}


class SentenceParts{
    String path;
    boolean XIsFirst;
    String fileName;
    LinkedTreeMap<String, Double> leftMap;
    LinkedTreeMap<String, Double> rightMap;

    public InputStream getFile() {
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        String location = "results/output/" + this.fileName;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("diamlior321").key(location).build();
        return s3.getObject(getObjectRequest);
    }

    public SentenceParts(String s){
        path = s.substring(2, s.length() - 2);
        XIsFirst = (s.charAt(0) == 'X');
        fileName = "X_" + path + "_Y.json";
        try (InputStream reader = getFile())
        {
            HashMap<String, Object> sentence1Map;
            sentence1Map = new Gson().fromJson(String.valueOf(reader), HashMap.class);
            if(XIsFirst){
                leftMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotX");
                rightMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotY");
            }
            else{
                leftMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotY");
                rightMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotX");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
