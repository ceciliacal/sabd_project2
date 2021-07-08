package query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;
import utils.Ship;

import java.text.SimpleDateFormat;
import java.util.*;

public class DistanceAggregate implements AggregateFunction<Ship, AccumulatorQuery3, OutputDistanceQuery3> {


    @Override
    public AccumulatorQuery3 createAccumulator() {
        return new AccumulatorQuery3();
    }

    @Override
    public AccumulatorQuery3 add(Ship ship, AccumulatorQuery3 acc) {
        System.out.println("---add");
        Date ts = ship.getTsDate();
        Double lon = ship.getLon();
        Double lat = ship.getLat();


        acc.addNewPosition(ts, lat, lon);
        return acc;
    }

    @Override
    public OutputDistanceQuery3 getResult(AccumulatorQuery3 acc) {
        //qui devo fare calcolo della distanza.
        //ordinare key (x sicurezza) e poi prendere primo e ultimo elemento della mappa
        //e calcola la distanza fra quelle due coordinate
        acc.getPositionTimestamp().forEach((k, v) -> System.out.println("k: " + k + " v: " + v));

        //sort
        List<Date> timestamps = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Map<Date, Tuple2<Double, Double>> sortedTimestamps = new TreeMap<>(acc.getPositionTimestamp());
        for (Map.Entry<Date, Tuple2<Double, Double>> entry : sortedTimestamps.entrySet()) {
            timestamps.add(entry.getKey());
        }

        //prendo primo e ultimo elemento
        Date firstTsDate = timestamps.get(0);
        Date lastTsDate = timestamps.get(timestamps.size() - 1);

        Double firstPositionLat = sortedTimestamps.get(firstTsDate)._1;
        Double firstPositionLon = sortedTimestamps.get(firstTsDate)._2;
        Double lastPositionLat = sortedTimestamps.get(lastTsDate)._1;
        Double lastPositionLon = sortedTimestamps.get(lastTsDate)._2;

        System.out.println("first ts:" + firstTsDate + "   lat: " + firstPositionLat + "  lon: " + firstPositionLon);
        System.out.println("last ts:" + lastTsDate + "   lat: " + lastPositionLat + "  lon: " + lastPositionLon);

        //calcolo distanza euclidea: sqr[(firstLat- lastLat)^2 + (firstLon -lastLon)^2]
        Double lat = Math.abs(lastPositionLat - firstPositionLat);
        Double lon = Math.abs(lastPositionLon - firstPositionLon);
        Double distance = Math.sqrt((lat * lat) + (lon * lon));


        return new OutputDistanceQuery3(distance);
    }

    @Override
    public AccumulatorQuery3 merge(AccumulatorQuery3 accumulatorQuery3, AccumulatorQuery3 acc1) {
        return null;
    }
}

