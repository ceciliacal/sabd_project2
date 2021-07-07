package utils;

import java.text.SimpleDateFormat;

public interface Config {
    public static String KAFKA_BROKERS = "localhost:9091";
    public static String CLIENT_ID = "myclient";
    //public static String datasetPath = "test";
    //public static String datasetPath = "prj2_dataset";
    //public static String datasetPath = "datasetPROVA2000";
    public static String datasetPath = "dataset Prova 8000";
    public static String TOPIC1 = "prova";
    public static String TOPIC_Q1 = "QUERY1";
    public static Double accTime = 0.0;
    public static Integer TIME_DAYS = 2;
    public static Integer TIME_DAYS_7 = 7;
    public static Integer TIME_DAYS_30 = 30;
    public static Integer TIME_MINS = 0;
    public static Integer TIME_SEC = 0;
    public static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"), new SimpleDateFormat("dd-MM-yy HH:mm")};
    public static String ARMY_TYPE = "army";
    public static String PASSENGERS_TYPE = "passenger transport";
    public static String CARGO_TYPE = "cargo";
    public static String OTHERS_TYPE = "others";
}
