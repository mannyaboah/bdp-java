package cscie88.week4;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cscie88.week2.LogLine;
import cscie88.week2.LogLineParser;

public class UniqueUrlCounterMapper extends Mapper<Object, Text, Text, Text> {

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd:HH");

    private Text dateHourKey = new Text();

    private Text URL = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        LogLine parsedLogLine = LogLineParser.parseLine(value.toString());

        String dateHrStr = parsedLogLine.getEventDateTime().format(formatter);

        String url = parsedLogLine.getUrl();

        dateHourKey.set(dateHrStr);
        URL.set(url);

        context.write(dateHourKey, URL);
    }

}
