package query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Config;
import utils.DataEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class MyProcessWindowFunction extends ProcessWindowFunction <OutputQuery1, OutputQuery1, String, TimeWindow>{

    @Override
    public void process(String key, Context context, Iterable<OutputQuery1> iterable, Collector<OutputQuery1> out) throws Exception {

        OutputQuery1 res = iterable.iterator().next();
        Date date = new Date();
        date.setTime(context.window().getStart());
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, MMM d, yyyy hh:mm:ss a z");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        res.setDate(date);
        res.setCellId(key);
        out.collect(res);
        System.out.println("---res: " + res);


    }
}


 /*

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String,Double>> values, Collector<Tuple2<String,Double>> out) throws Exception {


        System.out.println("------------------------STO IN PROCESS !!!!!!!!!!!!!!!------------------------");
        Tuple2<String,Double> avg = values.iterator().next();
        out.collect(new Tuple2<>(avg.f0, avg.f1));
        /*
        for (Tuple2<String,Double> value : values) {
            System.out.println("-=-= tupla: "+value.f0+", "+value.f1);
        }


        Tuple2<String,Double> result = values.iterator().next();
        //result.setTemperature(sum / count);
        out.collect(result);

         */




