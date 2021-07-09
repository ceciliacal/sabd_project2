package query3;

import connectionToKafka.MyProducer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import query2.OutputQuery2;
import utils.Ship;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Query3 {

    public static void runQuery3(StreamExecutionEnvironment env, DataStream<Ship> stream) throws Exception {

        KeyedStream<Ship, String> keyedStream = stream.keyBy(line -> line.getTripId());

        DataStreamSink<String> oneHour =
                keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(+15)))
                .aggregate(new DistanceAggregate(), new ProcessWindowFunction<OutputDistanceQuery3, OutputDistanceQuery3, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<OutputDistanceQuery3> iterable, Collector<OutputDistanceQuery3> out) throws Exception {

                        System.out.println("start della window: "+context.window().getStart());
                        OutputDistanceQuery3 res = iterable.iterator().next();
                        Date date = new Date();
                        date.setTime(context.window().getStart());

                        res.setDate(date);
                        res.setTripId(key);
                        out.collect(res);
                        System.out.println("--IN WINDOWFUNCTION: ");
                        System.out.println("res  = "+res);

                    }
                })
                //faccio merge dei risultati di tutte le key nella time window
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(+15)))
                .aggregate(new AggregateFunction<OutputDistanceQuery3, FinalAccumulatorQuery3, FinalOutputQuery3>() {

                               @Override
                               public FinalAccumulatorQuery3 createAccumulator() {
                                   return new FinalAccumulatorQuery3();
                               }

                               @Override
                               public FinalAccumulatorQuery3 add(OutputDistanceQuery3 outputDistance, FinalAccumulatorQuery3 acc) {
                                   System.out.println("---add");
                                   acc.addToCalculateRank(outputDistance.getDistance(), outputDistance.getTripId());
                                   return acc;
                               }

                               @Override
                               public FinalOutputQuery3 getResult(FinalAccumulatorQuery3 acc) {
                                   System.out.println("---result");
                                   acc.getDistanceTripId().forEach((k, v) -> System.out.println("AM k: " + k + " v: " + v));
                                   return new FinalOutputQuery3(acc.getDistanceTripId());
                               }

                               @Override
                               public FinalAccumulatorQuery3 merge(FinalAccumulatorQuery3 finalAccumulatorQuery3, FinalAccumulatorQuery3 acc1) {
                                   return null;
                               }
                           }
                        , new ProcessAllWindowFunction<FinalOutputQuery3, FinalOutputQuery3, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<FinalOutputQuery3> iterable, Collector<FinalOutputQuery3> out) throws Exception {

                                System.out.println("start della window: "+context.window().getStart());
                                FinalOutputQuery3 res = iterable.iterator().next();
                                Date date = new Date();
                                date.setTime(context.window().getStart());
                                System.out.println("date IN WINDOWFUNCTION = "+date);

                                res.setDate(date);
                                out.collect(res);

                                System.out.println("--- FINAL res: " + res);

                            }
                        })
                .map((MapFunction<FinalOutputQuery3, String>) myOutput -> FinalOutputQuery3.writeQuery3Result(myOutput, 1))
                .addSink(new FlinkKafkaProducer<String>("QUERY3",
                                new utils.ProducerStringSerializationSchema("QUERY3"),
                                MyProducer.getFlinkPropAsProducer(),
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query3Result");


        DataStreamSink<String> twoHours =
                keyedStream
                        .window(TumblingEventTimeWindows.of(Time.hours(2), Time.hours(+1)))
                        .aggregate(new DistanceAggregate(), new ProcessWindowFunction<OutputDistanceQuery3, OutputDistanceQuery3, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<OutputDistanceQuery3> iterable, Collector<OutputDistanceQuery3> out) throws Exception {

                                System.out.println("start della window: "+context.window().getStart());
                                OutputDistanceQuery3 res = iterable.iterator().next();
                                Date date = new Date();
                                date.setTime(context.window().getStart());

                                res.setDate(date);
                                res.setTripId(key);
                                out.collect(res);
                                System.out.println("--IN WINDOWFUNCTION: ");
                                System.out.println("res  = "+res);

                            }
                        })
                        //faccio merge dei risultati di tutte le key nella time window
                        .windowAll(TumblingEventTimeWindows.of(Time.hours(2), Time.hours(+1)))
                        .aggregate(new AggregateFunction<OutputDistanceQuery3, FinalAccumulatorQuery3, FinalOutputQuery3>() {

                                       @Override
                                       public FinalAccumulatorQuery3 createAccumulator() {
                                           return new FinalAccumulatorQuery3();
                                       }

                                       @Override
                                       public FinalAccumulatorQuery3 add(OutputDistanceQuery3 outputDistance, FinalAccumulatorQuery3 acc) {
                                           System.out.println("---add");
                                           acc.addToCalculateRank(outputDistance.getDistance(), outputDistance.getTripId());
                                           return acc;
                                       }

                                       @Override
                                       public FinalOutputQuery3 getResult(FinalAccumulatorQuery3 acc) {
                                           System.out.println("---result");
                                           acc.getDistanceTripId().forEach((k, v) -> System.out.println("AM k: " + k + " v: " + v));
                                           return new FinalOutputQuery3(acc.getDistanceTripId());
                                       }

                                       @Override
                                       public FinalAccumulatorQuery3 merge(FinalAccumulatorQuery3 finalAccumulatorQuery3, FinalAccumulatorQuery3 acc1) {
                                           return null;
                                       }
                                   }
                                , new ProcessAllWindowFunction<FinalOutputQuery3, FinalOutputQuery3, TimeWindow>() {
                                    @Override
                                    public void process(Context context, Iterable<FinalOutputQuery3> iterable, Collector<FinalOutputQuery3> out) throws Exception {

                                        System.out.println("start della window: "+context.window().getStart());
                                        FinalOutputQuery3 res = iterable.iterator().next();
                                        Date date = new Date();
                                        date.setTime(context.window().getStart());
                                        System.out.println("date IN WINDOWFUNCTION = "+date);

                                        res.setDate(date);
                                        out.collect(res);

                                        System.out.println("--- FINAL res: " + res);

                                    }
                                })
                        .map((MapFunction<FinalOutputQuery3, String>) myOutput -> FinalOutputQuery3.writeQuery3Result(myOutput, 2))
                        .addSink(new FlinkKafkaProducer<String>("QUERY3",
                                new utils.ProducerStringSerializationSchema("QUERY3"),
                                MyProducer.getFlinkPropAsProducer(),
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                        .name("query3Result");


        /*
                .addSink(new FlinkKafkaProducer<String>("QUERY2",
                        new utils.ProducerStringSerializationSchema("QUERY2"),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2Result");
         */



        env.execute("query3");

    }

}
