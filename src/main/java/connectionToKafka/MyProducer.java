package connectionToKafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Config;

public class MyProducer {
    //devo mettere topic e le properties per il kafka client (bootstrap servers)

    private static final String datasetPath = "data/prj2_dataset.csv";


    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void PublishMessages2() throws IOException {

        AtomicInteger count = new AtomicInteger(); //conto a che linea sto
        List<String> csvTimestamps = new ArrayList<>();

        //Putting it in a try catch  block to catch File read exceptions.
        try {
            //Setting up a Stream to our CSV File.
            Stream<String> FileStream = Files.lines(Paths.get(datasetPath));


            //Here we are going to read each line using Foreach loop on the FileStream object
            FileStream.forEach(line -> {

                System.out.println("------------------------START----------------------");
                count.getAndIncrement();
                System.out.println("count = " + count);


                //System.out.println("line: " + line);
                String[] fields = line.split(",");
                String key = fields[0];
                String[] value = Arrays.copyOfRange(fields, 1, fields.length);
                String tsCurrent = value[6];
                System.out.println("tsCurrent = " + tsCurrent);


                System.out.println("line: " + line);
                System.out.println("key: " + key);
                System.out.println("value: " + Arrays.toString(value));

                line = String.join(",", value);  //trasforma da String[] a String

                //qui dovrei fare un metodo che ritarda l'invio delle tuple al broker in base
                // alla differenza tra i timestamp di due msg consecutivi
                if (count.get() > 1) {
                    csvTimestamps.add(tsCurrent);
                    simulateStream(tsCurrent, csvTimestamps, count.get());
                }


            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("lista csvTimeStamps: "+ csvTimestamps);


    }

    private static void PublishMessages() {
        //Defining a Producer Object with set Properties.
        final Producer<String, String> producer = createProducer();

        //Putting it in a try catch  block to catch File read exceptions.
        try {
            //Setting up a Stream to our CSV File.
            Stream<String> FileStream = Files.lines(Paths.get(datasetPath));
            AtomicInteger count = new AtomicInteger(); //conto a che linea sto
            List<String> csvTimestamps = new ArrayList<>();

            //Here we are going to read each line using Foreach loop on the FileStream object
            FileStream.forEach(line -> {

                System.out.println("------------------------START----------------------");
                count.getAndIncrement();
                System.out.println("count = "+count);


                //System.out.println("line: " + line);
                String[] fields = line.split(",");
                String key = fields[0];
                String[] value = Arrays.copyOfRange(fields,1, fields.length);
                String tsCurrent = value[6];
                System.out.println("tsCurrent = "+tsCurrent);


                System.out.println("line: " + line);
                System.out.println("key: " + key);
                System.out.println("value: " + Arrays.toString(value));

                line = String.join(",",value);  //trasforma da String[] a String

                //qui dovrei fare un metodo che ritarda l'invio delle tuple al broker in base
                // alla differenza tra i timestamp di due msg consecutivi
                if (count.get()>1) {
                    csvTimestamps.add(tsCurrent);
                    String csvTimestamp = value[6];
                    String csvTimestamp1 = "";
                    String csvTimestamp2 = "";
                    simulateStream(tsCurrent, csvTimestamps, count.get());
                }

                //invio dei messaggi
                final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(
                        Config.TOPIC1, key, line
                );

                // Send a record and set a callback function with metadata passed as argument.
                producer.send(CsvRecord, (metadata, exception) -> {
                    if(metadata != null){
                        //Printing successful writes.
                        System.out.println("CsvData: -> "+ CsvRecord.key()+" | "+ CsvRecord.value());
                    }
                    else{
                        //Printing unsuccessful writes.
                        System.out.println("Error Sending Csv Record -> "+ CsvRecord.value());
                    }
                });

                String tsLast = value[6];
                System.out.println("tsLast = "+tsLast);
                System.out.println("------------------------END----------------------");
            });


            producer.close();
        } catch ( IOException e) {
            e.printStackTrace();
        }

    }

    //metodo che calcola tempo di ritardo dell'invio dei messaggi
    private static void simulateStream(String tsCurrent, List<String> tsList, int i) {

        String tsLast = tsList.get(i);

        //converto da stringa a data i ts
        //tempo di cui ritardare mettilo come var globale cosi la cambio x i test


    }


    public static void main(String[] args) throws IOException {

        PublishMessages2();

    }
}
