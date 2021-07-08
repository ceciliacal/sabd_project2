package query3;


import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FinalOutputQuery3 {

    private Date date;
    private Map<Double, String> result;

    public FinalOutputQuery3(TreeMap<Double, String> distanceTripId){
        int count = 0;
        int max = 4;

        for (Map.Entry<Double, String> entry : distanceTripId.entrySet()){
            if (count>max) {
                break;
            }
            else{
                //con la put sovrascrivo ma posso farlo perché tanto la chiave è la distanza percorsa
                //quindi in caso di sovrascrizione ottengo l'ultimo trip id che ha percorso una distanza maggiore
                //e scarto quelli precedenti
                result.put(entry.getKey(), entry.getValue());
                count++;
            }

        }

    }

    public Map<Double, String> getResult() {
        return result;
    }

    public void setResult(Map<Double, String> result) {
        this.result = result;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "FinalOutputQuery3{" +
                "date=" + date +
                ", result=" + result +
                '}';
    }
}
