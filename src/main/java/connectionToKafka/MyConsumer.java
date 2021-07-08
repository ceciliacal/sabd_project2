package connectionToKafka;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import query1.Query1;
import utils.MyMapFunction;
import query2.Query2;
import utils.Config;

import java.time.Duration;
import java.util.Properties;
import utils.Ship;


public class MyConsumer {


    public static void main(String[] args) throws Exception {


        FlinkKafkaConsumer<String> consumer = createConsumer();

        /*
        WatermarkStrategy<Ship> strategy = WatermarkStrategy.<Ship>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                                //.withIdleness(Duration.ofDays(5))
                                                .withTimestampAssigner((data, ts) -> data.getTsDate().getTime());

         */

        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));



        StreamExecutionEnvironment env = createEnviroment(consumer);


        DataStream<Ship> stream = env.addSource(consumer)
                .map(new MyMapFunction())
                .returns(Ship.class);

        //Query1.runQuery1(env, stream);
        Query2.runQuery2(env, stream);

    }

    public static FlinkKafkaConsumer<String> createConsumer() throws Exception {
        // Create properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using properties
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Config.TOPIC1, new SimpleStringSchema(), props);

        System.out.println("--Query2 creato consumer--");
        return myConsumer;

    }

    public static StreamExecutionEnvironment createEnviroment(FlinkKafkaConsumer<String> consumer){
        System.out.println("sto in create env QUERY2--");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }


}