import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.avro.data.Json;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class CalculateSimilarities {
    public static void main(String[] args) throws Exception {
        S3Helper s3Helper = new S3Helper();
        HashMap<String, Double> sentencesSimMap = new HashMap<>();
        File file = new File("input.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        PrintWriter writer = new PrintWriter("output.txt", "UTF-8");
        String st;

        System.out.println("Starting to calculate similarities, it might take a while...");

        while ((st = br.readLine()) != null) {
            String[] sents = st.split("\t");
            SentenceParts sentence1 = new SentenceParts(sents[0], s3Helper);
            SentenceParts sentence2 = new SentenceParts(sents[1], s3Helper);
            double result = 0;
            if (!sentence1.failed() && !sentence2.failed()) {
                double leftSim = calculateSlotSim(sentence1.leftMap, sentence2.leftMap);
                System.out.println("left sim " + leftSim);
                double rightSim = calculateSlotSim(sentence1.rightMap, sentence2.rightMap);
                System.out.println("right sim " + rightSim);
                result = Math.sqrt(leftSim * rightSim);
            }
            writer.println(st + "\t" + result);
            System.out.println(st + "\t" + result);
        }

        writer.close();

        System.out.println("Finished calculating!");
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
    String lib = "big/output/results/output/";
    String path;
    boolean XIsFirst;
    String fileName;
    LinkedTreeMap<String, Double> leftMap;
    LinkedTreeMap<String, Double> rightMap;

    public boolean failed(){
        return leftMap == null;
    }

    public SentenceParts(String s, S3Helper s3Helper){
        path = s.substring(2, s.length() - 2);
        XIsFirst = (s.charAt(0) == 'X');
        fileName = lib + path + ".json";
        try (InputStream reader = s3Helper.getFile(fileName))
        {
            HashMap<String, Object> sentence1Map;
            sentence1Map = new Gson().fromJson(IOUtils.toString(reader, StandardCharsets.UTF_8), HashMap.class);
            if(XIsFirst){
                leftMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotX");
                rightMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotY");
            }
            else{
                leftMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotY");
                rightMap = (LinkedTreeMap<String, Double>) sentence1Map.get("SlotX");
            }
            System.out.println("found " + path);
        } catch (IOException | NoSuchKeyException e) {
            leftMap = null;
            rightMap = null;
            System.out.println("not found " + path);
        }
    }
}
