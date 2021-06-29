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
import utils.Config;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import utils.DataEntity;

import static utils.Config.dateFormats;


public class MyConsumer {


    public static void main(String[] args) throws Exception {

        FlinkKafkaConsumer<String> consumer = createConsumer();
        StreamExecutionEnvironment env = createEnviroment(consumer);
        dataPrep(env, consumer);


    }

    //Consumer<String, String>
    public static FlinkKafkaConsumer<String> createConsumer() throws Exception {
        // Create properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using properties

        System.out.println("consumer working");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Config.TOPIC1, new SimpleStringSchema(), props);
        myConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));


        return myConsumer;

        //ora devo fare funzione per query1 che che come input riceve il DataStream però prima devo settare setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    }

    public static StreamExecutionEnvironment createEnviroment(FlinkKafkaConsumer<String> consumer){

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }


        
    public static void dataPrep(StreamExecutionEnvironment env, FlinkKafkaConsumer<String> consumer) throws Exception {

        System.out.println(" data prep: ");

        DataStream<DataEntity> stream = env.addSource(consumer).
        flatMap(new FlatMapFunction<String, DataEntity> () {

            @Override
            public void flatMap(String line, Collector<DataEntity> collector) throws Exception {

            String[] values = line.split(",");
            Date date = null;
            for (SimpleDateFormat dateFormat: dateFormats) {
                try {
                    date = dateFormat.parse(values[7]);
                    break;
                } catch (ParseException ignored) { }
            }

            DataEntity data = new DataEntity(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[2]), Double.parseDouble(values[3]), Double.parseDouble(values[4]), values[7]);
            System.out.println("DataEntity: "+data.getShipId()+", "+ data.getShipType()+", "+data.getSpeed()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp() );

            collector.collect(data);


        };


        }).name("bho");


        stream.print();
        env.execute("ingestingData");




        return;
    }
}
