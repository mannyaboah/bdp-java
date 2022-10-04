package cscie88.week4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TimeRangeCounterJob {

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("Usage: java EventCounterMRJob <input_dir> <output_dir> <start_time> <end_time>");
            System.exit(-1);
        }
        // create a new MR job
        Configuration conf = new Configuration();
        conf.set("start_time", args[2]);
        conf.set("end_time", args[3]);
        Job job = Job.getInstance(conf, "Unique Visitor counts per date_hour_url");

        job.setJarByClass(TimeRangeCounterJob.class);

        job.setMapperClass(TimeRangeCounterMapper.class);

        job.setCombinerClass(TimeRangeCounterReducer.class);

        job.setReducerClass(TimeRangeCounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
