package query1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import utils.Config;
import utils.DataEntity;

public class MyProcessWindowFunction extends ProcessWindowFunction <DataEntity, String, String, TimeWindow>{
    @Override
    public void process(String s, Context context, Iterable<DataEntity> iterable, Collector<String> collector) throws Exception {

        System.out.println("-----------MERDA-----------");

        int count_army = 0;
        int count_passengerTransport = 0;
        int count_cargo = 0;
        int count_others = 0;

        int totalCount = 0;

        for (DataEntity value:iterable){
            totalCount ++;

            if (value.getShipType().equals(Config.ARMY_TYPE)){
                count_army++;
            }
            if (value.getShipType().equals(Config.PASSENGERS_TYPE)){
                count_passengerTransport++;
            }
            if (value.getShipType().equals(Config.CARGO_TYPE)){
                count_cargo++;
            }
            if (value.getShipType().equals(Config.OTHERS_TYPE)){
                count_others++;
            }

        }



    }

}


 /*

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String,Double>> values, Collector<Tuple2<String,Double>> out) throws Exception {


        System.out.println("------------------------STO IN PROCESS !!!!!!!!!!!!!!!------------------------");
        Tuple2<String,Double> avg = values.iterator().next();
        out.collect(new Tuple2<>(avg.f0, avg.f1));
        /*
        for (Tuple2<String,Double> value : values) {
            System.out.println("-=-= tupla: "+value.f0+", "+value.f1);
        }


        Tuple2<String,Double> result = values.iterator().next();
        //result.setTemperature(sum / count);
        out.collect(result);

         */




