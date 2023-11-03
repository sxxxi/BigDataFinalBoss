package ca.sheridancollege.bautisse.utils;

import ca.sheridancollege.bautisse.model.LogFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class Mappers {
    public static class DurationFileFilter extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        public static final DateTimeFormatter ARG_DATETIME_FORMAT = DateTimeFormatter.ISO_DATE_TIME;
        public static final DateTimeFormatter FILE_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        public static class Args {
            public static final String DURATION_START = "0";
            public static final String DURATION_END = "1";
            public static final String OUTPUT_KEY_INDEX = "2";
            public static final String OUTPUT_VALUE_INDEX = "3";

            public static final String INPUT_DATE_INDEX = "4";
            public static final String INPUT_TIME_INDEX = "5";
            public static final String INPUT_DT_FORMAT = "6";
        }

        @Override
        protected void map(
                LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context
        ) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            LocalDateTime durationStart;
            LocalDateTime durationEnd;

            if (key.get() == 0) return;

            try {
                durationStart = LocalDateTime.parse(config.get(Args.DURATION_START), ARG_DATETIME_FORMAT);
                durationEnd = LocalDateTime.parse(config.get(Args.DURATION_END), ARG_DATETIME_FORMAT);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Provided dates has wrong format. Use yyyy-MM-ddTHH:mm:ss");
            }

            try {
                DateTimeFormatter dtFormat = DateTimeFormatter.ofPattern(config.get(Args.INPUT_DT_FORMAT));
                String[] fields = value.toString().split("\\s+");

                int outKeyIndex = config.getInt(Args.OUTPUT_KEY_INDEX, -1);
                int outValueIndex = config.getInt(Args.OUTPUT_VALUE_INDEX, -1);
                int dateIndex = config.getInt(Args.INPUT_DATE_INDEX, -1);
                int timeIndex = config.getInt(Args.INPUT_TIME_INDEX, -1);

                if (outKeyIndex < 0 || outValueIndex < 0 || dateIndex < 0 || timeIndex < 0)
                    throw new IllegalArgumentException("Must pass the index number of the output key and output value");

                LocalDateTime fileDate = LocalDateTime.parse(fields[dateIndex] + fields[timeIndex], dtFormat);

                if (fileDate.isAfter(durationStart) && fileDate.isBefore(durationEnd)) {
                    int outKey = Integer.parseInt(fields[outKeyIndex]);
                    double outVal = Double.parseDouble(fields[outValueIndex]);
                    context.write(new IntWritable(outKey), new DoubleWritable(outVal));
                }

            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("The passed date time format pattern is invalid", e);
            }
        }
    }

    public static class FileNameMapper extends Mapper<LongWritable, Text, Text, LogFile> {
        private static final DateTimeFormatter WEATHER_DT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH");
        private static final DateTimeFormatter CONSUMPTION_DT_FORMAT = DateTimeFormatter.ISO_DATE_TIME;

        @Override
        protected void map(
                LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, LogFile>.Context context
        ) throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0)) || value == null || context == null) return;

            String sourceName = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.")[0];
            LogFile file;
            String absolutePath;

            if (sourceName.startsWith("consumption_")) {
                file = LogFile.from("consumption.txt", value.toString(), 2, 3, CONSUMPTION_DT_FORMAT);
                absolutePath = "/energydata/" + file.createDate.getYear() + "/" + file.createDate.getMonthValue() + "/" + file.fileName;
            } else if (sourceName.startsWith("Weather_")) {
                file = LogFile.from("weather.txt", value.toString(), 0, 1, WEATHER_DT_FORMAT);
                absolutePath = "/weatherdata/" + file.createDate.getYear() + "/" + file.fileName;
            } else return;

            // Only allow data from 2015
            if (file.createDate.getYear() < 2015) return;

            context.write(new Text(absolutePath), file);
        }
    }
}
