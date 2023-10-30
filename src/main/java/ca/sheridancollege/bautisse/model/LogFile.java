package ca.sheridancollege.bautisse.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogFile implements Serializable, Writable {
    public String fileName;
    public LocalDateTime createDate;
    public String content;

    public LogFile(String fileName, LocalDateTime createDate, String content) {
        this.fileName = fileName;
        this.createDate = createDate;
        this.content = content;
    }

    public LogFile() {}

    public static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    public static final DateTimeFormatter dateTimeStringFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static LogFile from(String content, Integer dateIdx, Integer timeIdx, DateTimeFormatter inputDateTimeFormat, String filePrefix) {
        String [] columns = content.split("\\s+");

        String dateStr = columns[dateIdx];
        String timeStr = columns[timeIdx];

        LocalDateTime dateTime = LocalDateTime.parse(dateStr + "T" + timeStr, inputDateTimeFormat);
        String timestamp = dateTimeStringFormat.format(dateTime);

        return new LogFile(
                filePrefix + "_" + timestamp + ".txt",
                dateTime,
                content
        );
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(fileName);
        out.writeUTF(content);
        out.writeUTF(createDate.format(dateTimeFormat));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.fileName = in.readUTF();
        this.content = in.readUTF();
        this.createDate = LocalDateTime.parse(in.readUTF(), dateTimeFormat);
    }
}
