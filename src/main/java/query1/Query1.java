package query1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.DataEntity;

public class Query1 {


    public static void runQuery1(DataStream<DataEntity> stream) {

        stream.filter(line -> line.getSea().equals("mediterraneoOccidentale"))
                .keyBy(line -> line.getCell())
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction());     //windowFunction che calcola media sui valori in quella window
                //devo fare funzione con window da passare come secondo param a aggregate per prendere il ts di quando inizia calcolo media

    }
}
