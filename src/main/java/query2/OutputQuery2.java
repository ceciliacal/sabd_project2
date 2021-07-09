package query2;

import org.apache.flink.api.java.tuple.Tuple2;
import utils.Config;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class OutputQuery2 {

    private Date date;
    private String typeSea;

    //key: cella value: num navi
    private List<Tuple2<List<String>, Integer>> amRank;
    private List<Tuple2<List<String>, Integer>> pmRank;

    public void setAmRank(List<Tuple2<List<String>, Integer>> amRank) {
        this.amRank = amRank;
    }

    public void setPmRank(List<Tuple2<List<String>, Integer>> pmRank) {
        this.pmRank = pmRank;
    }

    public OutputQuery2(Map<String, List<String>> am, Map<String, List<String>> pm) {

        calculateAmRank(am);
        calculatePmRank(pm);
    }

    public void calculateAmRank(Map<String, List<String>> am){

        Map<Integer, List<String>> swappedKeyValue = new HashMap<>();
        this.amRank = new ArrayList<>();
        int count = 0;
        int max = 2;

        //io ho idCella, listaIDs -> mi servono le 3 liste con size maggiore

        for (Map.Entry<String, List<String>> entry : am.entrySet()) {
            List<String> values = new ArrayList<>();
            int size = entry.getValue().size();
            if (!swappedKeyValue.containsKey(size)){
                values.add(entry.getKey());
                swappedKeyValue.put(size, values);
            }
            //se c'è già la chiave che è la size del num di navi, devo appendere la cella ai values già presenti
            else{
                List<String> valuesAlreadyInside = swappedKeyValue.get(size);

                valuesAlreadyInside.add(entry.getKey());
                swappedKeyValue.put(size, valuesAlreadyInside);

            }
        }

        //key: num navi + alto, value: cella
        Map<Integer,List<String>> sortedCells = new TreeMap<>(Collections.reverseOrder());
        sortedCells.putAll(swappedKeyValue);

        System.out.println("OUTPUTQUERY2: sortedCells COMPLETA di AM: "+sortedCells);

        //prendo primi tre elementi da sortedCells
        for (Map.Entry<Integer, List<String>> entry : sortedCells.entrySet()){
            if (count>max) {
                break;
            }
            else{
                amRank.add(new Tuple2<>(entry.getValue(), entry.getKey()));
                count++;
            }

        }

    }

    public void calculatePmRank(Map<String, List<String>> pm){

        Map<Integer, List<String>> swappedKeyValue = new HashMap<>();
        this.pmRank = new ArrayList<>();

        int count = 0;
        int max = 2;

        //io ho idCella, listaIDs -> mi servono le 3 liste con size maggiore

        for (Map.Entry<String, List<String>> entry : pm.entrySet()) {
            List<String> values = new ArrayList<>();
            int size = entry.getValue().size();

            //se non c'è già quella chiave (cioè quel numero di navi con associato le celle)
            if (!swappedKeyValue.containsKey(size)){

                values.add(entry.getKey());
                swappedKeyValue.put(size, values);
            }
            //se c'è già la chiave che è la size del num di navi, devo appendere la cella ai values già presenti
            else{
                List<String> valuesGiaPresenti = swappedKeyValue.get(size);

                valuesGiaPresenti.add(entry.getKey());
                swappedKeyValue.put(size, valuesGiaPresenti);

            }

        }

        //key: num navi + alto, value: cella
        Map<Integer, List<String>> sortedCells = new TreeMap<>(Collections.reverseOrder());
        sortedCells.putAll(swappedKeyValue);

        System.out.println("OUTPUTQUERY2: sortedCells COMPLETA di PM: "+sortedCells);

        //prendo primi tre elementi da sortedCells
        for (Map.Entry<Integer, List<String>> entry : sortedCells.entrySet()){
            if (count>max) {
                break;
            }
            else{
                pmRank.add(new Tuple2<>(entry.getValue(), entry.getKey()));
                count++;
            }

        }

    }

    public static String writeQuery2Result_2(OutputQuery2 myOutput, int days) throws IOException {

        System.out.println("sto in writeQuery2Result: ");

        String outputPath = "results/"+Config.datasetPath+"_"+days+"_QUERY2.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getTypeSea());

        sb.append(",");
        sb.append("00:00-11:59");

        for (int i=0; i<myOutput.amRank.get(0).f0.size();i++){
            sb.append(",");
            sb.append(myOutput.amRank.get(0).f0.get(i));
        }

        sb.append(",");
        sb.append("12:00-23:59");

        for (int i=0; i<myOutput.pmRank.size();i++){
            sb.append(",");
            sb.append(myOutput.pmRank.get(i).f0);
        }
        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }

    public static String writeQuery2Result(OutputQuery2 myOutput, int days) throws IOException {

        System.out.println("sto in writeQuery2Result: ");

        String outputPath = "results/"+Config.datasetPath+"_"+days+"_QUERY2.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getTypeSea());

        sb.append(",");
        sb.append("00:00-11:59");

        for (int i=0; i<myOutput.amRank.size();i++){
            sb.append(",");
            sb.append(myOutput.amRank.get(i).f0);
        }

        sb.append(",");
        sb.append("12:00-23:59");

        for (int i=0; i<myOutput.pmRank.size();i++){
            sb.append(",");
            sb.append(myOutput.pmRank.get(i).f0);
        }
        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }

    @Override
    public String toString() {
        return "OutputQuery2{" +
                "date=" + date +
                ", typeSea='" + typeSea + '\'' +
                ", amRank=" + amRank +
                ", pmRank=" + pmRank +
                '}';
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getTypeSea() {
        return typeSea;
    }

    public void setTypeSea(String typeSea) {
        this.typeSea = typeSea;
    }

}
