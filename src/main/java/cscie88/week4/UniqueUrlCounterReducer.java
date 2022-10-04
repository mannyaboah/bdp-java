package cscie88.week4;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueUrlCounterReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text dateHrUrlKey, Iterable<Text> urls, Context context)
            throws IOException, InterruptedException {

        Set<String> set = new HashSet<>();

        for (Text url : urls) {
            set.add(url.toString());
        }

        result.set(String.valueOf(set.size()));

        context.write(dateHrUrlKey, result);
    }
}
