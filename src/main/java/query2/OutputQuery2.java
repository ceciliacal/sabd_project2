package query2;

import utils.Config;

import java.text.SimpleDateFormat;
import java.util.*;

public class OutputQuery2 {

    private Date date;
    private String typeSea;

    //key: cella value: num navi
    private Map<String, Integer> amRank;
    private Map<String, Integer> pmRank;

    public OutputQuery2(Map<String, List<String>> am, Map<String, List<String>> pm) {

        calculateAmRank(am);
        calculatePmRank(pm);
    }

    public void calculateAmRank(Map<String, List<String>> am){

        Map<Integer, String> swappedKeyValue = new HashMap<>();
        this.amRank = new HashMap<>();
        int count = 0;
        int max = 2;

        //io ho idCella, listaIDs -> mi servono le 3 liste con size maggiore

        for (Map.Entry<String, List<String>> entry : am.entrySet()) {
            int size = entry.getValue().size();
            swappedKeyValue.put(size, entry.getKey());
        }

        //key: num navi + alto, value: cella
        Map<Integer,String> sortedCells = new TreeMap<>(Collections.reverseOrder());
        sortedCells.putAll(swappedKeyValue);

        System.out.println("OUTPUTQUERY2: sortedCells COMPLETA di AM: "+sortedCells);

        //prendo primi tre elementi da sortedCells
        for (Map.Entry<Integer, String> entry : sortedCells.entrySet()){
            if (count>max) {
                break;
            }
            else{
                amRank.put(entry.getValue(), entry.getKey());
                count++;
            }

        }

    }

    public void calculatePmRank(Map<String, List<String>> pm){

        Map<Integer, String> swappedKeyValue = new HashMap<>();
        this.pmRank = new HashMap<>();

        int count = 0;
        int max = 2;

        //io ho idCella, listaIDs -> mi servono le 3 liste con size maggiore

        for (Map.Entry<String, List<String>> entry : pm.entrySet()) {
            int size = entry.getValue().size();
            swappedKeyValue.put(size, entry.getKey());
        }

        //key: num navi + alto, value: cella
        Map<Integer,String> sortedCells = new TreeMap<>(Collections.reverseOrder());
        sortedCells.putAll(swappedKeyValue);

        System.out.println("OUTPUTQUERY2: sortedCells COMPLETA di PM: "+sortedCells);

        //prendo primi tre elementi da sortedCells
        for (Map.Entry<Integer, String> entry : sortedCells.entrySet()){
            if (count>max) {
                break;
            }
            else{
                pmRank.put(entry.getValue(), entry.getKey());
                count++;
            }

        }

    }

    public static String writeQuery2Result(OutputQuery2 myOutput){

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

        sb.append("===Finestra temporale di "+ Config.TIME_DAYS_7+" giorni: ");
        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getTypeSea());
        sb.append(",");
        sb.append("00:00-11:59");
        sb.append(",");
        sb.append(myOutput.getAmRank());
        sb.append(",");
        sb.append("12:00-23:59");
        sb.append(",");
        sb.append(myOutput.getPmRank()+"===");

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

    public Map<String, Integer> getAmRank() {
        return amRank;
    }

    public void setAmRank(Map<String, Integer> amRank) {
        this.amRank = amRank;
    }

    public Map<String, Integer> getPmRank() {
        return pmRank;
    }

    public void setPmRank(Map<String, Integer> pmRank) {
        this.pmRank = pmRank;
    }
}
