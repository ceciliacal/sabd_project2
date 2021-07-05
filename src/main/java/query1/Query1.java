package query1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Config;
import utils.DataEntity;
import java.util.Properties;

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
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate( new AverageAggregate(),
                            new MyProcessWindowFunction())
                .map((MapFunction<OutputQuery1, String>) myOutput -> {

                    System.out.println("-----------------------");
                    System.out.println("ts: "+myOutput.getDate());
                    System.out.println("cell: "+myOutput.getCellId());
                    System.out.println("set: "+myOutput.getCountType().entrySet());

                    return OutputQuery1.writeQuery1Result(myOutput);

                    //System.out.println("type army: "+(double)(myOutput.getCountType().get(Config.ARMY_TYPE)/Config.TIME_DAYS_7));
                    //System.out.println("set: "+myOutput.getCountType().entrySet());
                })
                /*
                .addSink(new FlinkKafkaProducer<String>(Config.TOPIC_Q1,
                        new ProducerStringSerializationSchema(Config.TOPIC_Q1),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1Result");

                 */

                .print();

        env.execute("query1");

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