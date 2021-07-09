package query1;

import utils.Config;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class OutputQuery1 {

    private Date date;
    private String cellId;
    private Map<String, Integer> countType;


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


    public static String writeQuery1Result(OutputQuery1 myOutput, int days) throws FileNotFoundException {
        //scrive questo (esempio): 3> 2015-04-09,C20,army,0.29,others,0.57

        String outputPath = "results/"+Config.datasetPath+"_"+days+"_QUERY1.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getCellId());


        myOutput.getCountType().forEach((k,v) -> {
            sb.append(",");sb.append(k).append(",").append(String.format(Locale.ENGLISH, "%.2g", (double)v/days));
        });
        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }

}
