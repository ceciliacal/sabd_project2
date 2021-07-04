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
import utils.Config;

import java.time.Duration;
import java.util.Properties;
import utils.DataEntity;


public class MyConsumer {


    public static void main(String[] args) throws Exception {

        FlinkKafkaConsumer<String> consumer = createConsumer();
        WatermarkStrategy<DataEntity> strategy = WatermarkStrategy.<DataEntity>forBoundedOutOfOrderness(Duration.ofHours(1)).withIdleness(Duration.ofHours(1)).withTimestampAssigner((data, ts) -> data.getTsDate().getTime());
        StreamExecutionEnvironment env = createEnviroment(consumer);
        //DataStream<DataEntity> stream = dataPrep(env, consumer);
        Query1.runQuery1(strategy, env, consumer);
        //env.execute();



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
        //myConsumer.WatermarkStrategy.<DataEntity>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withIdleness(Duration.ofMinutes(1)).withTimestampAssigner((data, ts) -> data.getTsDate().getTime());

        WatermarkStrategy<DataEntity> strategy = WatermarkStrategy.<DataEntity>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withIdleness(Duration.ofMinutes(1)).withTimestampAssigner((data, ts) -> data.getTsDate().getTime());
        //myConsumer.assignTimestampsAndWatermarks(strategy);


        return myConsumer;

        //ora devo fare funzione per query1 che che come input riceve il DataStream però prima devo settare setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        /*

        .process(new KeyedProcessFunction<String, DataEntity, DataEntity>() {
                    @Override
                    public void processElement(DataEntity dataEntity, Context context, Collector<DataEntity> collector) throws Exception {
                        System.out.println("dopo trasformazioni: "+dataEntity.getShipId()+", "+dataEntity.getShipType()+", "+dataEntity.getSea()+", "+dataEntity.getCell());
                    }
                });

         */

    }

    public static StreamExecutionEnvironment createEnviroment(FlinkKafkaConsumer<String> consumer){

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }



        
    public static DataStream<DataEntity> dataPrep(StreamExecutionEnvironment env, FlinkKafkaConsumer<String> consumer) throws Exception {

        //nelle funzioni di flink, nei <> si mette il tipo dei dati sia dei param di input che del tipo di ritorno
        // String qui è parametro di input e Collector<DataEntity> è il tipo di ritorno
        //e poi nell'override si specifica params e tipo nel modo classico, riprendendoli da quelli riportati in <>

        //to process data simultaneously on multiple concurrent instances, group the data through the key by operation
        //con keyed stream gli operatori sono stateful ??

        DataStream<DataEntity> stream = env.addSource(consumer).
                flatMap(new FlatMapFunction<String, DataEntity> () {

                    @Override
                    public void flatMap(String line, Collector<DataEntity> collector) throws Exception {

                        String[] values = line.split(",");

                        DataEntity data = new DataEntity(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]), Double.parseDouble(values[4]), values[7]);
                        System.out.println("DataEntity: "+data.getShipId()+", "+ data.getShipType()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp()+", "+data.getCell()+", "+data.getTsDate()+", "+data.getSea()+"\n");

                        collector.collect(data);


                    };


                }).name("dataEntity_stream");


        //stream.print();
        //env.execute("ingestingData");


        return stream;
    }
}
