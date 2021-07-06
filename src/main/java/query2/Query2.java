package query2;

import connectionToKafka.MyProducer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.Config;
import utils.Ship;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Query2 {

    public static void runQuery2(WatermarkStrategy<Ship> strategy, StreamExecutionEnvironment env, DataStream<Ship> stream) throws Exception {

        stream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Ship>forBoundedOutOfOrderness(Duration.ofDays(7))
                        .withTimestampAssigner((ship, timestamp) -> ship.getTsDate().getTime()))
                .keyBy(line -> line.getSea())
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new RankAggregate(), new Query2ProcessWindowFunction())
                .map((MapFunction<OutputQuery2, String>) myOutput -> OutputQuery2.writeQuery2Result(myOutput))

                /*
                .addSink(new FlinkKafkaProducer<String>("QUERY2",
                        new utils.ProducerStringSerializationSchema("QUERY2"),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

                 */


                .name("query2Result")


                .print();

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
