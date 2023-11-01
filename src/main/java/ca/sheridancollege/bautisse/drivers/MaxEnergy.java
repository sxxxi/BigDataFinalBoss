package ca.sheridancollege.bautisse.drivers;

import ca.sheridancollege.bautisse.utils.Mappers;
import ca.sheridancollege.bautisse.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class MaxEnergy {
    public static final DateTimeFormatter CONSUME_DATE_FORMAT = DateTimeFormatter.ISO_DATE_TIME;
    public static final DateTimeFormatter FILE_NAME_TEMPORAL_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private static final Pattern pattern = Pattern.compile("\\d+");

        @Override
        protected void map(
                LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context
        ) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            LocalDateTime consumeDateTime = LocalDateTime.parse(
                    conf.get(Args.CONSUME_DATE_TIME),
                    CONSUME_DATE_FORMAT
            ).withMinute(0).withSecond(0);

            LocalDateTime consumePlusOneH = consumeDateTime.plusHours(1);

            // xxxx_##_yyyyMMddHHmmss.txt -> xxxx_##_yyyyMMddHHmmss
            String fileNameDate = ((FileSplit) context.getInputSplit()).getPath().getName()
                    .split("\\.")[0]
                    .split("_")[2];

            // compare consumeDateTime and the file date
            LocalDateTime fileDate = LocalDateTime.parse(fileNameDate, FILE_NAME_TEMPORAL_FORMAT);

            if (fileDate.isAfter(consumeDateTime) && fileDate.isBefore(consumePlusOneH)) {
                String[] content = value.toString().split("\\s");

                int houseIdInt = Integer.parseInt(content[0]);
                double conDouble = Double.parseDouble(content[4]);

                IntWritable houseId = new IntWritable(houseIdInt);
                DoubleWritable consumed = new DoubleWritable(conDouble);

                context.write(houseId, consumed);
            }
        }

        public static class Args {
            public static final String CONSUME_DATE_TIME = "CONSUME_DATE_TIME";
        }
    }

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(
                IntWritable key, Iterable<DoubleWritable> values,
                Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context
        ) throws IOException, InterruptedException {
            double max = 0.0;

            for (DoubleWritable consumption : values) {
                max = Math.max(max, consumption.get());
            }

            context.write(key, new DoubleWritable(max));
        }
    }

    public static void execute(
            String conRootDir, String durationStart, String durationEnd, String outputDir
    ) throws Exception {
        Path outDir = new Path(outputDir);

        Configuration config = new Configuration();
        config.set(Mappers.DurationFileFilter.Args.DURATION_START, durationStart);
        config.set(Mappers.DurationFileFilter.Args.DURATION_END, durationEnd);
        config.setInt(Mappers.DurationFileFilter.Args.OUTPUT_KEY_INDEX, 1);
        config.setInt(Mappers.DurationFileFilter.Args.OUTPUT_VALUE_INDEX, 4);

        Job job = Job.getInstance(config, "Get all household's max consumption");

        // Just in case it requires files from multiple data sources.
        FileInputFormat.addInputPaths(job, Utils.commaSeperatedDirs(conRootDir, DateTimeFormatter.ISO_DATE_TIME, durationStart, durationEnd));
        FileOutputFormat.setOutputPath(job, outDir);

        job.setJarByClass(Mappers.DurationFileFilter.class);
        job.setMapperClass(Mappers.DurationFileFilter.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
