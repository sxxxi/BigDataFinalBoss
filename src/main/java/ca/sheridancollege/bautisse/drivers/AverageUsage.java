package ca.sheridancollege.bautisse.drivers;

import ca.sheridancollege.bautisse.utils.Mappers;
import ca.sheridancollege.bautisse.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AverageUsage {
    private static final int HOUSE_ID_IDX = 1;
    private static final int CONSUMPTION_IDX = 4;

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
            AtomicReference<Double> sum = new AtomicReference<>(0.0);
            AtomicInteger qty = new AtomicInteger();

            values.forEach( value -> {
                sum.updateAndGet(v -> (v + value.get()));
                qty.addAndGet(1);
            });

            double average = sum.get() / qty.get();

            context.write(key, new DoubleWritable(average));
        }
    }

    /**
     * Only supports durations within a month. I need to implement the behaviour when changing directories.
     * @param inputRoot
     * @param durationStart
     * @param durationEnd
     * @param output
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void execute(
            String inputRoot, String durationStart, String durationEnd, String output
    ) throws Exception {
        // create a job that creates another that has a custom input directory.
        LocalDateTime consumedDateTime = LocalDateTime.parse(durationStart, MaxEnergy.CONSUME_DATE_FORMAT);
        String targetDirString = Paths.get(
                Integer.toString(consumedDateTime.getYear()),
                Integer.toString(consumedDateTime.getMonthValue())
        ).toString();

        Path inPath = new Path(inputRoot);
        Path targetDir = new Path(inPath, targetDirString);
        Path outDir = new Path(output);

        Configuration config = new Configuration();
        config.set(Mappers.DurationFileFilter.Args.DURATION_START, durationStart);
        config.set(Mappers.DurationFileFilter.Args.DURATION_END, durationEnd);
        config.setInt(Mappers.DurationFileFilter.Args.OUTPUT_KEY_INDEX, HOUSE_ID_IDX);
        config.setInt(Mappers.DurationFileFilter.Args.OUTPUT_VALUE_INDEX, CONSUMPTION_IDX);
        config.set(Mappers.DurationFileFilter.Args.INPUT_DT_FORMAT, "yyyy-MM-ddHH:mm:ss");
        config.setInt(Mappers.DurationFileFilter.Args.INPUT_DATE_INDEX, 2);
        config.setInt(Mappers.DurationFileFilter.Args.INPUT_TIME_INDEX, 3);

        Job job = Job.getInstance(config, "Get all household's max consumption");

        job.setJarByClass(Mappers.DurationFileFilter.class);

        job.setMapperClass(Mappers.DurationFileFilter.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job, Utils.commaSeperatedDirs(inputRoot, DateTimeFormatter.ISO_DATE_TIME, durationStart, durationEnd));
        FileOutputFormat.setOutputPath(job, outDir);

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );

    }
}
