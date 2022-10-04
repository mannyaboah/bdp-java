package cscie88.week4;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TimeRangeCounterReducer extends Reducer<Text, Text, Text, Text> {

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH");

    private Text result = new Text();

    public void reduce(Text dateHourCtryKey, Iterable<Text> countries, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> map = new HashMap<>();

        Configuration conf = context.getConfiguration();
        String startTime = conf.get("start_time");
        String endTime = conf.get("end_time");

        String[] dateHr = dateHourCtryKey.toString().split(",");

        LocalDateTime currentDateHr = LocalDateTime.parse(dateHr[0], formatter);
        LocalDateTime start = LocalDateTime.parse(startTime.toString(), formatter);
        LocalDateTime end = LocalDateTime.parse(endTime.toString(), formatter);

        if ((currentDateHr.isEqual(start) || currentDateHr.isAfter(start)) &&
                (currentDateHr.isEqual(end) || currentDateHr.isBefore(end))) {

            for (Text cntry : countries) {
                int count = map.containsKey(cntry.toString()) ? map.get(cntry.toString()) : 0;
                map.put(cntry.toString(), count + 1);
            }

            map.forEach((k, v) -> {
                result.set(String.valueOf(v));
                try {
                    context.write(dateHourCtryKey, result);
                } catch (IOException e) {
                    System.out.println("IOException Occurred!");
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    System.out.println("InterruptedException Occurred!");
                    e.printStackTrace();
                }
            });
        }

    }

}
