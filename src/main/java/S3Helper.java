import org.json.simple.JSONObject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class S3Helper{
    S3Client s3;
    String bucketName;
    public S3Helper(){
        Region region = Region.US_WEST_2;
        s3 = S3Client.builder()
                .region(region)
                .build();
        this.bucketName = "thecoolbucketthatismine";
    }
    public void writeToS3(JSONObject obj, String name){
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest
                    .builder()
                    .bucket(bucketName)
                    .key("output/results/" + name)
                    .build();
            s3.putObject(putObjectRequest,
                    RequestBody.fromBytes(obj.toString().getBytes(StandardCharsets.UTF_8)));
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    public InputStream getFile(String fileName) {
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        String location = "output/results/output/" + fileName;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(location).build();
        return s3.getObject(getObjectRequest);
    }
}