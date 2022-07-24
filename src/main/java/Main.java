import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce1_SlotX");
        job.setJarByClass(MapReducer1.class);
        job.setMapperClass(MapReducer1.Mapper1.class);
        job.setMapOutputKeyClass(SentenceOne.class);
        job.setMapOutputValueClass(DoubleWritable3.class);
        job.setReducerClass(MapReducer1.Reducer1.class);
        job.setPartitionerClass(MapReducer1.SlotXPartitioner.class);
        job.setOutputKeyClass(SentenceOne.class);
        job.setOutputValueClass(DoubleWritable3.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output_1"));



        ControlledJob jobControl1 = new ControlledJob(job.getConfiguration());
        jobControl1.setJob(job);


        JobControl jobControl = new JobControl("job-control");
        jobControl.addJob(jobControl1);

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
