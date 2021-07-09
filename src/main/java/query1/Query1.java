package query1;

import connectionToKafka.MyProducer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.Config;
import utils.Ship;


public class Query1 {


    public static void runQuery1(StreamExecutionEnvironment env, DataStream<Ship> stream) throws Exception {

        KeyedStream<Ship, String> keyedStream = stream
                .filter(line -> line.getSea().equals("mediterraneoOccidentale"))
                .keyBy(line -> line.getCell());


        DataStreamSink<String> oneWeek =
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(Config.TIME_DAYS_7), Time.days(+5)))
                .aggregate( new AverageAggregate(),
                            new Query1ProcessWindowFunction())
                .map((MapFunction<OutputQuery1, String>) myOutput -> OutputQuery1.writeQuery1Result(myOutput, Config.TIME_DAYS_7))
                .name("query1Result")
                .addSink(new FlinkKafkaProducer<String>(Config.TOPIC_Q1,
                        new utils.ProducerStringSerializationSchema(Config.TOPIC_Q1),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));



                //.print()
                //.setParallelism(1)

        DataStreamSink<String> fourWeeks =
                keyedStream
                        .window(TumblingEventTimeWindows.of(Time.days(Config.TIME_MONTH), Time.days(+12)))
                        .aggregate( new AverageAggregate(),
                                new Query1ProcessWindowFunction())
                        .map((MapFunction<OutputQuery1, String>) myOutput -> OutputQuery1.writeQuery1Result(myOutput, Config.TIME_MONTH))
                        .name("query1Result")
                        .addSink(new FlinkKafkaProducer<String>(Config.TOPIC_Q1,
                                new utils.ProducerStringSerializationSchema(Config.TOPIC_Q1),
                                MyProducer.getFlinkPropAsProducer(),
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE));



        env.execute("query1");

        System.out.println("----sto in runQuery1");
    }


}