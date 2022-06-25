import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;


public class Main {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce1_SlotX");
        job.setJarByClass(MapReducer1.class);
        job.setMapperClass(MapReducer1.Mapper1.class);
        job.setMapOutputKeyClass(Sentence.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(MapReducer1.Reducer1.class);
        job.setPartitionerClass(MapReducer1.SlotXPartitioner.class);
        job.setOutputKeyClass(Sentence.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output_1"));

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MapReduce2_SlotY");
        job2.setJarByClass(MapReducer2.class);
        job2.setMapperClass(MapReducer2.Mapper2.class);
        job2.setMapOutputKeyClass(Sentence.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setReducerClass(MapReducer2.Reducer2.class);
        job2.setPartitionerClass(MapReducer2.SlotXPartitioner.class);
        job2.setOutputKeyClass(Sentence.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path("output_1"));
        FileOutputFormat.setOutputPath(job2, new Path("output_2"));

        ControlledJob jobControl1 = new ControlledJob(job.getConfiguration());
        jobControl1.setJob(job);
        ControlledJob jobControl2 = new ControlledJob(job2.getConfiguration());
        jobControl2.setJob(job2);

        JobControl jobControl = new JobControl("job-control");
        jobControl.addJob(jobControl1);
        jobControl.addJob(jobControl2);
        jobControl2.addDependingJob(jobControl1);


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
