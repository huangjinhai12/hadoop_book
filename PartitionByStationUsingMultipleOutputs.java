import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 多个文件输出示例
 * <h1>目标</h1>
 * <p>有多少个气象站，就输出多少个该气象站气温数据。</p>
 * @author Henry
 * Created at 2017年8月18日
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {
    // Mapper
    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final NcdcRecordParser mParser = new NcdcRecordParser();
        @Override
        protected void map(final LongWritable key, final Text value, final Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            mParser.parse(value);
            context.write(new Text(mParser.getStationId()), value);
        }
    }

    // Reducer
    static class StationReducer extends Reducer<Text, Text, NullWritable, Text> {
        MultipleOutputs<NullWritable, Text> multipleOutputs = null;

        @Override
        protected void setup(final Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(final Text arg0, final Iterable<Text> arg1, final Reducer<Text, Text, NullWritable, Text>.Context arg2) throws IOException, InterruptedException {
            for (final Text value : arg1) {
                multipleOutputs.write(NullWritable.get(), value, arg0.toString()/*baseOutputPath*/);
            }
        }

        @Override
        protected void cleanup(final Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(final String[] arg0) throws Exception {
        final Job job = JobBuilder.parseInputAndOutput(this, getConf(), arg0);
        if (job == null) {
            return -1;
        }

        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(StationReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    // Main
    public static void main(final String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
        System.exit(exitCode);
    }
}
