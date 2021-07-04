package query1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import utils.DataEntity;


public class MyMapFunction implements MapFunction<String, DataEntity> {


    @Override
    public DataEntity map(String line) throws Exception {

        String[] values = line.split(",");

        DataEntity data = new DataEntity(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]), Double.parseDouble(values[4]), values[7]);
        System.out.println("DataEntity: "+data.getShipId()+", "+ data.getShipType()+", "+data.getLon()+", "+data.getLat()+", "+data.getTimestamp()+", "+data.getCell()+", "+data.getTsDate()+", "+data.getSea()+"\n");

        return data;
    }
}

