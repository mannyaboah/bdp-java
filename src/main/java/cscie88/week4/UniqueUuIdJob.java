package cscie88.week4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UniqueUuIdJob {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: java EventCounterMRJob <input_dir> <output_dir>");
            System.exit(-1);
        }
        // create a new MR job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Unique Event(UUId) counts per date_hour_url");

        job.setJarByClass(UniqueUuIdJob.class);

        job.setMapperClass(UniqueUuIdMapper.class);

        job.setCombinerClass(UniqueUuIdReducer.class);

        job.setReducerClass(UniqueUuIdReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
