package query1;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class OutputQuery1 {

    private Map<String, Double> countType;
    private String cellId;
    private Date date;

    @Override
    public String toString() {
        return "OutputQuery1{" +
                "countType=" + countType +
                ", cellId='" + cellId + '\'' +
                ", date=" + date +
                '}';
    }

    public OutputQuery1(Map<String, Double> countType) {
        this.countType = countType;
        //this.countType = countType.get(countType.keySet());
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
