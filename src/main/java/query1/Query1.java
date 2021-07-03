package query1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.DataEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Query1 {


    public static void runQuery1(WatermarkStrategy<DataEntity> strategy, StreamExecutionEnvironment env, FlinkKafkaConsumer consumer) throws Exception {

        DataStream<DataEntity> stream = env.addSource(consumer).
                flatMap(new FlatMapFunction<String, DataEntity> () {

                    @Override
                    public void flatMap(String line, Collector<DataEntity> collector) {
                        String[] values = line.split(",");

                        DataEntity data = new DataEntity(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]), Double.parseDouble(values[4]), values[7]);
                        //System.out.println("DataEntity: "+data.getShipId()+", "+ data.getShipType()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp()+", "+data.getCell()+", "+data.getTsDate()+", "+data.getSea()+"\n");
                        collector.collect(data);

                    };

                }).returns(DataEntity.class);

        stream.filter(line -> line.getSea().equals("mediterraneoOccidentale"))
                .assignTimestampsAndWatermarks(strategy)
                //.assignTimestampsAndWatermarks(WatermarkStrategy.<DataEntity>forBoundedOutOfOrderness(Duration.ofDays(1)).withTimestampAssigner((dataEntity, l) -> dataEntity.getTsDate().getTime()))
                .keyBy(line -> line.getCell())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //.window(new TumblingEventTimeWindows<DataEntity, String, TimeWindow>()
                .aggregate(new AggregateFunction<DataEntity, AccumulatorQuery1, OutputQuery1>() {
                               @Override
                               public AccumulatorQuery1 createAccumulator() {
                                   return new AccumulatorQuery1();
                               }

                               @Override
                               public AccumulatorQuery1 add(DataEntity dataEntity, AccumulatorQuery1 accumulatorQuery1) {
                                   System.out.println("==dataentity: "+dataEntity);
                                   System.out.println("---add");
                                   Tuple2<String, Double> t = new Tuple2<>(dataEntity.getShipType(), 0.0);
                                   accumulatorQuery1.setTupla(t);
                                   return accumulatorQuery1;
                               }

                               @Override
                               public OutputQuery1 getResult(AccumulatorQuery1 accumulatorQuery1) {
                                   System.out.println("---result");
                                   return new OutputQuery1(accumulatorQuery1.getTupla().f0);
                               }

                               @Override
                               public AccumulatorQuery1 merge(AccumulatorQuery1 accumulatorQuery1, AccumulatorQuery1 acc1) {
                                   System.out.println("---merge");
                                   return accumulatorQuery1;
                               }
                           }, new ProcessWindowFunction<OutputQuery1, OutputQuery1, String, TimeWindow>() {

                               @Override
                               public void process(String s, Context context, Iterable<OutputQuery1> iterable, Collector<OutputQuery1> collector) throws Exception {
                                   OutputQuery1 res = iterable.iterator().next();
                                   Date date = new Date();
                                   date.setTime(context.window().getStart());
                                   SimpleDateFormat sdf = new SimpleDateFormat("EEE, MMM d, yyyy hh:mm:ss a z");
                                   sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

                                   res.setDate(date);
                                   System.out.println("---res: " + res);
                               }
                           }).print();



                        env.execute("aiuto");

                /*
                .flatMap(new FlatMapFunction<DataEntity, Object>() {
                    @Override
                    public void flatMap(DataEntity data, Collector<Object> collector) throws Exception {
                        System.out.println("--bho: "+data.getShipId()+", "+ data.getShipType()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp()+", "+data.getCell()+", "+data.getTsDate()+", "+data.getSea());
                        collector.collect(data);
                    }
                });

                 */

                /*
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new MyProcessWindowFunction());

                 */


                /*
                .aggregate(new AverageAggregate())
                .flatMap(new FlatMapFunction<Tuple2<String, Double>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, Double> bho, Collector<String> out)  {
                        System.out.println("--bho: "+bho);
                        String bho2 = "merda";
                        out.collect(bho2);
                    }
                }).print();

                 */




                //windowFunction che calcola media sui valori in quella window
                //devo fare funzione con window da passare come secondo param a aggregate per prendere il ts di quando inizia calcolo media
                //.aggregate(new AverageAggregate(), new MyProcessWindowFunction())

                /*
                .process(new KeyedProcessFunction<Tuple2<String,Double>, Tuple2<String,Double>>() {
                    public void processElement(Tuple2<String,Double> o, KeyedProcessFunction.Context context, Collector<Tuple2<String,Double>> collector) throws Exception {

                  }

                 */
        








        System.out.println("----sto in runQuery1");
    }
}

/*
    private static class WrapWithWeek
            extends ProcessWindowFunction<DataEntity, Tuple2<String,Double>, TimeWindow> {

        public void process(Type key,
                            KeyedProcessFunction.Context context,
                            Iterable<DataEntity> reducedEvents,
                            Collector<Tuple2<String,Double> out) {
            Long sum = reducedEvents.iterator().next();
            out.collect(new Tuple3<Type, Long, Long>(key, context.window.getStart(), sum));
        }

        @Override
        public void process(Object o, Context context, Iterable iterable, Collector collector) throws Exception {

        }
    }
}

 */



