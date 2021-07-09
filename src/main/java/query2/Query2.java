package query2;

import connectionToKafka.MyProducer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.Config;
import utils.ProducerStringSerializationSchema;
import utils.Ship;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Query2 {

    public static void runQuery2(DataStream<Ship> stream) throws Exception {

        System.out.println("--sto in runQuery2--");

        KeyedStream<Ship, String> keyedStream = stream.keyBy(line -> line.getSea());

        DataStreamSink<String> oneWeek =
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(Config.TIME_DAYS_7), Time.days(+5)))
                .aggregate(new RankAggregate(), new Query2ProcessWindowFunction())
                .map((MapFunction<OutputQuery2, String>) myOutput -> OutputQuery2.writeQuery2Result(myOutput, Config.TIME_DAYS_7))
                .name("query2Result")
                .addSink(new FlinkKafkaProducer<String>("QUERY2",
                        new utils.ProducerStringSerializationSchema("QUERY2"),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

                //.print();


        DataStreamSink<String> fourWeeks =
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(Config.TIME_MONTH), Time.days(+12)))
                .aggregate(new RankAggregate(), new Query2ProcessWindowFunction())
                .map((MapFunction<OutputQuery2, String>) myOutput -> OutputQuery2.writeQuery2Result(myOutput, Config.TIME_MONTH))
                .name("query2Result")
                .addSink(new FlinkKafkaProducer<String>("QUERY2",
                        new ProducerStringSerializationSchema("QUERY2"),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        //.print();

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
