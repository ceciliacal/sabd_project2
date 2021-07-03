package utils;

import java.text.SimpleDateFormat;

public interface Config {
    public static String KAFKA_BROKERS = "localhost:9091";
    public static String CLIENT_ID = "myclient";
    public static String TOPIC1 = "prova";
    public static Double accTime = 0.0;
    public static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"), new SimpleDateFormat("dd-MM-yy HH:mm")};
    public static String ARMY_TYPE = "army";
    public static String PASSENGERS_TYPE = "passenger transport";
    public static String CARGO_TYPE = "cargo";
    public static String OTHERS_TYPE = "others";
}
