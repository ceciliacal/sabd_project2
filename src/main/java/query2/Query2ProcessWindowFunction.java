package query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Query2ProcessWindowFunction extends ProcessWindowFunction<OutputQuery2, OutputQuery2, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<OutputQuery2> iterable, Collector<OutputQuery2> out) throws Exception {

        System.out.println("start della window: "+context.window().getStart());
        OutputQuery2 res = iterable.iterator().next();
        Date date = new Date();
        date.setTime(context.window().getStart());
        System.out.println("date IN WINDOWFUNCTION = "+date);
        /*
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

         */

        res.setDate(date);
        res.setTypeSea(key);
        out.collect(res);

        System.out.println("---res: " + res);
    }

}
