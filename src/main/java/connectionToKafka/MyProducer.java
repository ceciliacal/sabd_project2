package connectionToKafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Config;

public class MyProducer {
    //devo mettere topic e le properties per il kafka client (bootstrap servers)

    private static final String datasetPath = "data/prj2_dataset.csv";


    public static Producer<String, String> setProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }



    public static void readFromCsv(){


    }

    private static void PublishMessages() {
        //Defining a Producer Object with set Properties.
        final Producer<String, String> myKafkaProducer = setProducerProperties();

        //Putting it in a try catch  block to catch File read exceptions.
        try {
            //Setting up a Stream to our CSV File.
            Stream<String> FileStream = Files.lines(Paths.get(datasetPath));
            //Here we are going to read each line using Foreach loop on the FileStream object
            FileStream.forEach(line -> {

                //System.out.println("line: " + line);
                String[] fields = line.split(",");
                //System.out.println("id: " + fields[0]);




                //The topic the record will be appended to
                //The key that will be included in the record
                //The record contents
                final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(
                        Config.TOPIC1, fields[0], line
                );

                // Send a record and set a callback function with metadata passed as argument.
                myKafkaProducer.send(CsvRecord, (metadata, exception) -> {
                    if(metadata != null){
                        //Printing successful writes.
                        System.out.println("CsvData: -> "+ CsvRecord.key()+" | "+ CsvRecord.value());
                    }
                    else{
                        //Printing unsuccessful writes.
                        System.out.println("Error Sending Csv Record -> "+ CsvRecord.value());
                    }
                });

                // */
            });


            myKafkaProducer.close();
        } catch ( IOException e) {
            e.printStackTrace();
        }


    }



    public static void main(String[] args){

        PublishMessages();


    }
}
