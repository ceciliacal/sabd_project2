package query3;


import org.apache.flink.api.java.tuple.Tuple2;
import query2.OutputQuery2;
import utils.Config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class FinalOutputQuery3 {

    private Date date;
    private List<Tuple2<Double, String>> result2;


    public FinalOutputQuery3(Map<Double, String> distanceTripId){

        this.result2 = new ArrayList<>();

        int count = 0;
        int max = 4;

        //key: valore distanza piu alto, value: tripId
        Map<Double, String> sortedDistances = new TreeMap<>(distanceTripId);
        //sortedDistances.putAll(distanceTripId);

        System.out.println("OUTPUTQUERY3: sortedDistances : "+sortedDistances);

        for (Map.Entry<Double, String> entry : distanceTripId.entrySet()){
            if (count>max) {
                break;
            }
            else{
                //con la put sovrascrivo ma posso farlo perché tanto la chiave è la distanza percorsa
                //quindi in caso di sovrascrizione ottengo l'ultimo trip id che ha percorso una distanza maggiore
                //e scarto quelli precedenti
                //result.put(entry.getKey(), entry.getValue());
                result2.add(new Tuple2<>(entry.getKey(), entry.getValue()));

                count++;
            }

        }

    }


    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public List<Tuple2<Double, String>> getResult2() { return result2; }

    public void setResult2(List<Tuple2<Double, String>> result2) { this.result2 = result2; }

    public static String writeQuery3Result(FinalOutputQuery3 myOutput) throws IOException {

        System.out.println("sto in writeQuery3Result: ");

        String outputPath = "results/"+ Config.datasetPath+"_"+Config.TIME_MONTH+"_QUERY3.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd HH:mm"));

        sb.append(simpleDateFormat.format(timestamp));

        int count = 0;
        for (int i=0; i<myOutput.result2.size();i++){
            count = i+1;
            sb.append(",");
            sb.append("trip_"+count);
            sb.append(",");
            sb.append(myOutput.result2.get(i).f1);
            sb.append(",");
            sb.append("rating_"+count);
            sb.append(",");
            sb.append(myOutput.result2.get(i).f0);
        }

        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }

    @Override
    public String toString() {
        return "FinalOutputQuery3{" +
                "date=" + date +
                ", result=" + result2 +
                '}';
    }
}
