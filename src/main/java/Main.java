import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    public static void main(String[] args) throws Exception {
        S3Helper s3 = new S3Helper();
        String bucketPath = "s3://" + s3.bucketName + "/";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce1_SlotX");
        job.setJarByClass(MapReducer1.class);
        job.setMapperClass(MapReducer1.Mapper1.class);
        job.setMapOutputKeyClass(SentenceOneX.class);
        job.setMapOutputValueClass(DoubleWritable2.class);
        job.setReducerClass(MapReducer1.Reducer1.class);
        job.setPartitionerClass(MapReducer1.SlotXPartitioner.class);
        job.setCombinerClass(MapReducer1.Reducer1.class);
        job.setOutputKeyClass(SentenceOneX.class);
        job.setOutputValueClass(DoubleWritable2.class);
        MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput1"), TextInputFormat.class, MapReducer1.Mapper1.class);

        if (args.length > 0 && args[0].equals("big")) {
            bucketPath += "big/";
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput2"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput3"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput4"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput5"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput6"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput7"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput8"), TextInputFormat.class, MapReducer1.Mapper1.class);
            MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/projectinput/DIRTinput9"), TextInputFormat.class, MapReducer1.Mapper1.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(bucketPath + "output_1"));

        Job job2 = Job.getInstance(conf, "MapReduce2_SlotY");
        job2.setJarByClass(MapReducer2.class);
        job2.setMapperClass(MapReducer2.Mapper2.class);
        job2.setMapOutputKeyClass(SentenceOneY.class);
        job2.setMapOutputValueClass(DoubleWritable3.class);
        job2.setReducerClass(MapReducer2.Reducer2.class);
        job2.setPartitionerClass(MapReducer2.SlotYPartitioner.class);
        job2.setOutputKeyClass(SentenceOneY.class);
        job2.setOutputValueClass(DoubleWritable3.class);
        FileInputFormat.addInputPath(job2, new Path(bucketPath + "output_1"));
        FileOutputFormat.setOutputPath(job2, new Path(bucketPath + "output_2"));

        Job job3 = Job.getInstance(conf, "MapReduce3_final");
        job3.setJarByClass(MapReducer3.class);
        job3.setMapperClass(MapReducer3.Mapper3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(SlotMaps.class);
        job3.setReducerClass(MapReducer3.Reducer3.class);
        job3.setCombinerClass(MapReducer3.Combiner3.class);
        job3.setPartitionerClass(MapReducer3.FinalPartitioner.class);
        FileInputFormat.addInputPath(job3, new Path(bucketPath + "output_2"));
        FileOutputFormat.setOutputPath(job3, new Path(bucketPath + "output_3"));

        ControlledJob jobControl2 = new ControlledJob(job.getConfiguration());
        jobControl2.setJob(job2);

        ControlledJob jobControl3 = new ControlledJob(job.getConfiguration());
        jobControl3.setJob(job3);
        jobControl3.addDependingJob(jobControl2);

        JobControl jobControl = new JobControl("job-control");
        jobControl.addJob(jobControl2);
        jobControl.addJob(jobControl3);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        int code = 0;
        while (!jobControl.allFinished()) {
            code = jobControl.getFailedJobList().size() == 0 ? 0 : 1;
            Thread.sleep(1000);
        }
        System.exit(code);

    }
}
