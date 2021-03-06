package connectionToKafka;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import query1.Query1;
import query3.Query3;
import utils.MyMapFunction;
import query2.Query2;
import utils.Config;

import java.time.Duration;
import java.util.Properties;
import utils.Ship;


public class MyConsumer {


    public static void main(String[] args) throws Exception {

        FlinkKafkaConsumer<String> consumer = createConsumer();
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));
        StreamExecutionEnvironment env = createEnviroment();

        //creo lo stream di tipo "Ship" andando a splittare le righe
        //che vengono lette dal topic di kafka da parte del consumer con MyMapFunction
        DataStream<Ship> stream = env.addSource(consumer)
                .map(new MyMapFunction())
                //filtro tuple che sono comprese nelle celle
                .filter(line -> line.checkCell());

        DataStream<Ship> stream1 = stream;
        DataStream<Ship> stream2 = stream;
        DataStream<Ship> stream3 = stream;

        Query1.runQuery1(stream1);
        Query2.runQuery2(stream2);
        Query3.runQuery3(stream3);

        env.execute("sabd");

    }

    public static FlinkKafkaConsumer<String> createConsumer() throws Exception {
        // creazione properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // creazione consumer usando le properties
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Config.TOPIC1, new SimpleStringSchema(), props);

        System.out.println("---creato consumer--");
        return myConsumer;

    }

    public static StreamExecutionEnvironment createEnviroment(){
        System.out.println("--sto in create env--");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }


}