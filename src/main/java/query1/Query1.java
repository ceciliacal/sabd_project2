package query1;

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

import java.time.Duration;

public class Query1 {


    public static void runQuery1(StreamExecutionEnvironment env, DataStream<Ship> stream) throws Exception {

        stream
                .filter(line -> line.getSea().equals("mediterraneoOccidentale"))
                .keyBy(line -> line.getCell())
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(+5)))
                //.window(TumblingEventTimeWindows.of(Time.days(28), Time.days(+12)))
                .aggregate( new AverageAggregate(),
                            new Query1ProcessWindowFunction())
                .map((MapFunction<OutputQuery1, String>) myOutput -> {

                    /*
                    System.out.println("-----------------------");
                    System.out.println("ts: "+myOutput.getDate());
                    System.out.println("cell: "+myOutput.getCellId());
                    System.out.println("set: "+myOutput.getCountType().entrySet());


                     */
                    return OutputQuery1.writeQuery1Result(myOutput);

                    //System.out.println("type army: "+(double)(myOutput.getCountType().get(Config.ARMY_TYPE)/Config.TIME_DAYS_7));
                    //System.out.println("set: "+myOutput.getCountType().entrySet());
                })

                /*
                .addSink(new FlinkKafkaProducer<String>(Config.TOPIC_Q1,
                        new utils.ProducerStringSerializationSchema(Config.TOPIC_Q1),
                        MyProducer.getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


                 */
                .print()
                .setParallelism(1)

                .name("query1Result");





        env.execute("query1");

        System.out.println("----sto in runQuery1");
    }


}