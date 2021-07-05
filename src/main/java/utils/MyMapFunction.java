package utils;

import org.apache.flink.api.common.functions.MapFunction;


public class MyMapFunction implements MapFunction<String, Ship> {


    @Override
    public Ship map(String line) throws Exception {

        String[] values = line.split(",");

        Ship data = new Ship(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]), Double.parseDouble(values[4]), values[7]);
        System.out.println("DataEntity: "+data.getShipId()+", "+ data.getShipType()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp()+", "+data.getCell()+", "+data.getTsDate()+", "+data.getSea()+"\n");

        return data;
    }
}

