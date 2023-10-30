package ca.sheridancollege.bautisse.drivers;

import ca.sheridancollege.bautisse.model.LogFile;
import ca.sheridancollege.bautisse.utils.Mappers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class FileImport {

    public static class Reduce extends Reducer<Text, LogFile, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<LogFile> values, Reducer<Text, LogFile, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (LogFile value : values) {
                Path path = new Path(key.toString(), value.fileName);
                FileSystem fs = FileSystem.get(new Configuration());

                if (!fs.exists(path.getParent())) fs.mkdirs(path.getParent());

                FSDataOutputStream x = fs.create(path);

                BufferedWriter br = new BufferedWriter( new OutputStreamWriter(x, StandardCharsets.UTF_8) );
                br.write(value.content);
                br.close();
                x.close();

                context.write(key, new IntWritable(0));
            }
        }
    }

    public static void execute(
            String inputDir,
            String outputDir
    ) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Import datasets");

        job.setJarByClass(Mappers.FileNameMapper.class);

        job.setMapperClass(Mappers.FileNameMapper.class);
        job.setReducerClass(FileImport.Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogFile.class);

        job.setNumReduceTasks(4);

        Path targetDir = new Path(inputDir);
        Path outDir = new Path(outputDir);

        FileInputFormat.addInputPath(job, targetDir);
        FileOutputFormat.setOutputPath(job, outDir);

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}
