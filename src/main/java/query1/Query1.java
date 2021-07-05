package query1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Config;
import utils.DataEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class Query1 {


    public static void runQuery1(WatermarkStrategy<DataEntity> strategy, StreamExecutionEnvironment env, FlinkKafkaConsumer consumer) throws Exception {


        DataStream<DataEntity> stream = env.addSource(consumer)
                .map(new MyMapFunction())
                .returns(DataEntity.class)
                ;

        stream
                .filter(line -> line.getSea().equals("mediterraneoOccidentale"))
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(line -> line.getCell())
                .window(TumblingEventTimeWindows.of(Time.minutes(3)))
                //.process( new MyProcessWindowFunction())
                .aggregate( new AverageAggregate(),
                            new ProcessWindowFunction<OutputQuery1, OutputQuery1, String, TimeWindow>() {

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

                })


                .map((MapFunction<OutputQuery1, String>) myOutput -> {

                    // stampo il ts
                    System.out.println("-----------------------");
                    System.out.println("ts: "+myOutput.getDate());
                    System.out.println("cell: "+myOutput.getCellId());
                    System.out.println("set: "+myOutput.getCountType().entrySet());

                    return OutputQuery1.writeQuery1Result(myOutput);

                    //System.out.println("type army: "+(double)(myOutput.getCountType().get(Config.ARMY_TYPE)/Config.TIME_DAYS_7));
                    //System.out.println("-----CACCCAAAAAAAAAA---");
                    //System.out.println("set: "+myOutput.getCountType().entrySet());
                })
                .addSink(new FlinkKafkaProducer<String>(Config.TOPIC_Q1,
                        new ProducerStringSerializationSchema(Config.TOPIC_Q1),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1Result");

                //.print();

        env.execute("aiuto");

        System.out.println("----sto in runQuery1");
    }


    public static Properties getFlinkPropAsProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,Config.KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,Config.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());


        return properties;

    }

}
