package connectionToKafka;

import scala.Char;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Prova {

    public static void main(String[] args)  {

        List<Double> latSectors = new ArrayList<>();
        List<Double> lonSectors = new ArrayList<>();

        double lowerLat = 32.0;
        double upperLat = 45.0;
        double lowerLon = -6.0;
        double upperLon = 37.0;

        latSectors.add(lowerLat);
        lonSectors.add(lowerLon);

        double latRange = (upperLat-lowerLat);
        double lonRange = (upperLon-lowerLon);

        latRange = latRange / 10;

        System.out.println("LAT:");
        System.out.println(latRange);

        double sumLat = lowerLat;

        for (int i=0;i<9;i++){
            sumLat = sumLat + latRange;
            latSectors.add(sumLat);
        }

        latSectors.add(upperLat);

        System.out.println(latSectors);
        System.out.println("size: "+latSectors.size());
        System.out.println("LON:");

        lonRange = lonRange / 40;
        System.out.println(lonRange);

        double sumLon = lowerLon;

        for (int i=0;i<39;i++){
            sumLon = sumLon + lonRange;
            lonSectors.add(sumLon);
        }

        lonSectors.add(upperLon);

        System.out.println(lonSectors);
        System.out.println("size: "+lonSectors.size());

        //latitudine

        System.out.println("\n\n");
        double lat = 32.0;
        int indexLat = retrieveIndex(latSectors, lat);
        System.out.println("indexLat LAT = "+indexLat);
        String latLetter = getCharFromNumber(indexLat);
        System.out.println("settore LAT = "+latLetter);

        //longitudine

        double lon = -6.0;
        int indexLon = retrieveIndex(lonSectors, lon);
        String indexLonStr = String.valueOf(indexLon);
        System.out.println("settore LON = "+indexLon);

        String cell = latLetter.concat(indexLonStr);
        System.out.println("-- CELLA= "+cell);


    }

    public static int retrieveIndex(List<Double> list, double lon){

        int res =0;

        if (lon==list.get(0)){
            res = list.indexOf(list.get(0))+1;
            return res;

        }
        if (lon==list.get(list.size()-1)){
            double lastElem = list.get(list.size()-1);
            res = list.indexOf(lastElem);
            return res;

        }

        System.out.println("PRIMA FOR res= "+res);
        for (int i=0;i<list.size();i++){

            //System.out.println("NEL FOR res= "+res);


            //System.out.println("lonSectors.get(i)= "+lonSectors.get(i)+ " lonSectors.get(i+1)= "+lonSectors.get(i+1));
            if (lon > list.get(i) && lon < list.get(i+1)){

                res = i+1;
                //System.out.println("STO NELL IF i+1 "+res);
                break;
            }

        }

        //System.out.println("DOPO FOR res= "+res);
        return res;
    }

    public static String getCharFromNumber(int i){

        List<String> letterList = Arrays.asList(new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I","J"});
        String res = letterList.get(i-1);

        return res;



    }


    }
