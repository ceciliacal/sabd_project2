package query1;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputQuery1 {

    private Map<String, Integer> countType;
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

    public OutputQuery1(Map<String, List<String>> typeListId) {
        Map<String, Integer> res = new HashMap<>();
        System.out.println("---STO IN OUT QUERY1----"+typeListId.toString());
        for (Map.Entry<String, List<String>> entry : typeListId.entrySet()){
            res.put(entry.getKey(), entry.getValue().size());
            System.out.println("---STO IN OUT QUERY1 PUT: "+res.get(entry.getKey()));
            this.setCountType(res);
        }
    }

    public Map<String, Integer> getCountType() {
        return countType;
    }

    public void setCountType(Map<String, Integer> countType) {
        this.countType = countType;
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
