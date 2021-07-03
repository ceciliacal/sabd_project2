package query1;

import java.util.Date;

public class OutputQuery1 {

    private String cellId;
    private
    Date date;

    @Override
    public String toString() {
        return "Query1Outcome{" +
                "cellId='" + cellId + '\'' +
                ", date=" + date +
                '}';
    }

    public OutputQuery1(String cellId) {
        this.cellId = cellId;
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
