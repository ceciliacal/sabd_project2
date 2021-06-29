package connectionToKafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.Config;


public class MyProducer {
    //devo mettere topic e le properties per il kafka client (bootstrap servers)

    private static final String datasetPath = "data/prj2_dataset.csv";
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"), new SimpleDateFormat("dd-MM-yy HH:mm")};


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

        final Producer<String, String> producer = createProducer();

        //Putting it in a try catch  block to catch File read exceptions.
        try {
            //Setting up a Stream to our CSV File.
            Stream<String> FileStream = Files.lines(Paths.get(datasetPath));

            //Read each line using Foreach loop on the FileStream object and remove header
            FileStream.skip(1).forEach(line -> {

                Long sleepTime = null;

                System.out.println("------------------------START----------------------");
                count.getAndIncrement();
                System.out.println("count = " + count);


                //System.out.println("line: " + line);
                String[] fields = line.split(",");
                String key = fields[0];

                String[] value = Arrays.copyOfRange(fields, 1, fields.length);
                String tsCurrent = value[6];

                System.out.println("tsCurrent = " + tsCurrent);
                Date cacca = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        cacca = dateFormat.parse(tsCurrent);

                        System.out.println("---cacca---  "+cacca);

                        break;
                    } catch (ParseException ignored) { }
                }

                //System.out.println("csvTimestamps = "+csvTimestamps);

                System.out.println("line: " + line);


                //line = String.join(",", value);  //trasforma da String[] a String

                //qui dovrei fare un metodo che ritarda l'invio delle tuple al broker in base
                // alla differenza tra i timestamp di due msg consecutivi
                if (count.get() > 1) {

                    try {
                        System.out.println("-- tsCurrent = "+tsCurrent);
                        //TODO: sistema qua sotto. prende sempre stessa posizione
                        System.out.println("-- lastTs = "+csvTimestamps.get(count.get()-2));
                        System.out.println("-- list SIZE = "+csvTimestamps.size());
                        sleepTime = simulateStream(tsCurrent, csvTimestamps, count.get());

                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                }

                //aggiungo ts alla lista (a partire dal primo elemento)
                csvTimestamps.add(tsCurrent);

                /*
                if (count.get()>42){
                    exit(5);
                }

                 */

                if (sleepTime!=null){
                    try {
                        System.out.println("sleep time: "+sleepTime+" sec");
                        //TimeUnit.SECONDS.sleep(sleepTime);
                        TimeUnit.SECONDS.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
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

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("lista csvTimeStamps: "+ csvTimestamps.size());


    }


    //metodo che calcola tempo di ritardo dell'invio dei messaggi
    private static long simulateStream(String currentTs, List<String> tsList, int i) throws ParseException {

        System.out.println("-------------STO IN SIMULATE=============");

        String lastTs = tsList.get(i-2);

        System.out.println("i = "+i);
        System.out.println("currentTs = "+currentTs);
        System.out.println("lastTs = "+lastTs);
        /*
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        Date date = format.parse(tsLast);

         */

        Long lastTsTime = null;
        Long currentTsTime = null;
        for (SimpleDateFormat dateFormat: dateFormats) {
            try {
                lastTsTime = dateFormat.parse(lastTs).getTime();
                /*
                System.out.println("lastTsTime = "+lastTsTime);
                System.out.println("---PRIMA---  "+lastTsTime);
                 */
                break;
            } catch (ParseException ignored) { }
        }
        for (SimpleDateFormat dateFormat: dateFormats) {
            try {

                currentTsTime = dateFormat.parse(currentTs).getTime();
                /*
                System.out.println("currentTsTime = "+currentTsTime);
                System.out.println("---DOPO--- "+currentTsTime);
                 */

                break;
            } catch (ParseException ignored) { }
        }

        double diff = currentTsTime - lastTsTime;
        Long sleepTimeMillis =  (long) (diff*Config.accTime);
        //per una differenza di 2 min nei ts, dormo 2 sec
        Long sleepTimeSec = TimeUnit.MILLISECONDS.toMinutes(sleepTimeMillis);

        System.out.println("--- ritardo in MILLISEC: "+ sleepTimeMillis);
        System.out.println("--- ritardo in SECONDI: "+ sleepTimeSec);


        return sleepTimeSec;

    }


    public static void main(String[] args) throws IOException {

        PublishMessages2();

    }
}
