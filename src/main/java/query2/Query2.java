package query2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import query1.MyMapFunction;
import query1.OutputQuery1;
import utils.DataEntity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    public static void runQuery2(WatermarkStrategy<DataEntity> strategy, StreamExecutionEnvironment env, FlinkKafkaConsumer consumer) throws Exception {


        DataStream<DataEntity> stream = env.addSource(consumer)
                .map(new MyMapFunction())
                .returns(DataEntity.class);

        stream
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(line -> line.getSea())
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AggregateFunction<DataEntity, AccumulatorQuery2, OutputQuery2>() {
                    @Override
                    public AccumulatorQuery2 createAccumulator() {
                        return new AccumulatorQuery2();
                    }

                    @Override
                    public AccumulatorQuery2 add(DataEntity data, AccumulatorQuery2 acc) {
                        System.out.println("==dataentity: " + data);
                        System.out.println("---add");

                        String shipTimestamp = data.getTimestamp();
                        String[] tokens = shipTimestamp.split(" ");
                        String tripTime = tokens[1];
                        String amOrPm = checkDate(tripTime);

                        if (amOrPm.equals("am")) {
                            acc.addAM(data.getCell(), data.getShipId());
                        } else {
                            acc.addPM(data.getCell(), data.getShipId());
                        }

                        return acc;
                    }

                    @Override
                    public OutputQuery2 getResult(AccumulatorQuery2 acc) {
                        System.out.println("---result");
                        acc.getAm().forEach((k, v) -> System.out.println("k: " + k + " v: " + v));
                        acc.getPm().forEach((k, v) -> System.out.println("k: " + k + " v: " + v));
                        return new OutputQuery2(acc.getAm(), acc.getPm());

                    }

                    @Override
                    public AccumulatorQuery2 merge(AccumulatorQuery2 acc1, AccumulatorQuery2 acc2) {
                        return null;
                    }
                }, new ProcessWindowFunction<OutputQuery2, OutputQuery2, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<OutputQuery2> iterable, Collector<OutputQuery2> out) throws Exception {

                        OutputQuery2 res = iterable.iterator().next();
                        Date date = new Date();
                        date.setTime(context.window().getStart());
                        SimpleDateFormat sdf = new SimpleDateFormat("EEE, MMM d, yyyy hh:mm:ss a z");
                        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

                        res.setDate(date);
                        res.setTypeSea(key);
                        out.collect(res);

                        System.out.println("---res: "+res);

                    }
                }).print();

        env.execute("query2");


    }


    public static String checkDate(String dateTarget){
        String res = "";
        try {
            String string1 = "00:00";
            String string2 = "11:59";

            Date time1 = new SimpleDateFormat("HH:mm").parse(string1);
            Calendar calendar1 = Calendar.getInstance();
            calendar1.setTime(time1);
            calendar1.add(Calendar.DATE, 1);

            Date time2 = new SimpleDateFormat("HH:mm").parse(string2);
            Calendar calendar2 = Calendar.getInstance();
            calendar2.setTime(time2);
            calendar2.add(Calendar.DATE, 1);

            Date d = new SimpleDateFormat("HH:mm").parse(dateTarget);
            Calendar calendarTarget = Calendar.getInstance();
            calendarTarget.setTime(d);
            calendarTarget.add(Calendar.DATE, 1);

            Date x = calendarTarget.getTime();
            if (x.after(calendar1.getTime()) && x.before(calendar2.getTime())) {
                System.out.println(true);

                res = "am";
            }
            else{
                res = "pm";
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return res;
    }
}
