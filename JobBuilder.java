import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public final class JobBuilder {
    public static Job parseInputAndOutput(final Tool tool, final Configuration conf, final String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Please note that you have " + args.length + " parameters.");
            printUsage(tool, "<inout> <output>");
            return null;
        }
        final Job jobConf = Job.getInstance(conf);
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        return jobConf;
    }

    public static void printUsage(final Tool tool, final String extraArgsUsage) {
        System.err.printf("Usage: % [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
    }
}