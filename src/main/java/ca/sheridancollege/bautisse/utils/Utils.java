package ca.sheridancollege.bautisse.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;

public class Utils {
    public static LocalDateTime getFileNameDateTime(Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) {
        try {
            // xxxx_##_yyyyMMddHHmmss.txt -> xxxx_##_yyyyMMddHHmmss
            String fileNameDate = ((FileSplit) context.getInputSplit()).getPath().getName()
                    .split("\\.")[0]
                    .split("_")[2];

            // compare consumeDateTime and the file date
            return LocalDateTime.parse(fileNameDate, Mappers.DurationFileFilter.FILE_DATETIME_FORMAT);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("File has illegal format. Use fooo_32_yyyyMMddHHmmss.txt");
        }
    }

    public static String commaSeperatedDirs(
            String parent, DateTimeFormatter format, String startIsoDate, String endIsoDate
    ) throws Exception {
        LocalDate start = LocalDate.parse(startIsoDate, format);
        LocalDate end = LocalDate.parse(endIsoDate, format);

        YearMonthPath s = new YearMonthPath(start.getYear(), start.getMonthValue());
        YearMonthPath e = new YearMonthPath(end.getYear(), end.getMonthValue());

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        String parentFormatted = parent;

        while (parentFormatted.endsWith("/")) {
            parentFormatted = parent.substring(0, parent.lastIndexOf("/"));
        }

        for (String ymp : s.toPathUntil(e)) {
           if (first) {
               first = false;
           } else {
               sb.append(",");
           }
           sb.append(parent);
           sb.append(ymp);
//           throw new Exception(sb.toString() + "  " + parent);
        }

        return sb.toString();
    }

    private static class YearMonthPath implements Comparable<YearMonthPath> {
        private int year;
        private int month;

        public YearMonthPath(int year, int month) {
            this.year = year;
            this.month = month;
        }

        public String toPath() {
            return String.format("/%d/%d", year, month);
        }

        public Iterable<String> toPathUntil(YearMonthPath end) {
            ArrayList<String> paths = new ArrayList<>();
            while (this.compareTo(end) < 1) {
                paths.add(toPath());
                incrementMonth();
            }
            return paths;
        }

        public void incrementMonth() {
            if (month == 12) {
                year++;
                this.month = 1;
            } else {
                this.month++;
            }
        }

        @Override
        public int compareTo(YearMonthPath o) {
            int x = Integer.compare(year, o.year);
            if (x == 0) {
                return Integer.compare(month, o.month);
            } else {
                return x;
            }
        }
    }
}
