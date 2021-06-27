package connectionToKafka;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.Config;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) throws Exception {

        createConsumer();

    }

    //Consumer<String, String>
    public static void createConsumer() throws Exception {
        // Create properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using properties

        System.out.println("consumer working");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Config.TOPIC1, new SimpleStringSchema(), props);
        myConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

        DataStream<String> msgStream = env.addSource(myConsumer).name("stream");


        msgStream.print();
        env.execute("bho");

        //ora devo fare funzione per query1 che che come input riceve il DataStream però prima devo settare setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //System.out.println("---------STREAM: "+ Arrays.toString(msgStream));


        /*
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));
        DataStream<String> stream = env.addSource(myConsumer);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(Config.top));
        return consumer;

         */
        }

}
