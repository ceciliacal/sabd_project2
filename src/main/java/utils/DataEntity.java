package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static utils.Config.dateFormats;

public class DataEntity {

    String shipId;
    Integer shipTypeInt;
    String shipType;
    //double speed;
    double lon;
    double lat;
    String timestamp;
    String cell;
    Date tsDate;


    public DataEntity(String shipId, Integer shipTypeInt, double lon, double lat, String timestamp){
        this.shipId = shipId;
        this.shipTypeInt = shipTypeInt;
        //this.speed = speed;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;

        String cell = calculateCell(this.lat, this.lon);
        setCell(cell);

        String resultShipType = assignShipType(this.shipTypeInt);
        setShipType(resultShipType);

        Date date = stringToDate(this.timestamp);
        setTsDate(date);
        //this.cell = this.calculateCell(this.lat, this.lon);



    }

    public Date stringToDate(String myDate){

        Date date = null;
        for (SimpleDateFormat dateFormat: dateFormats) {
            try {
                date = dateFormat.parse(myDate);
                break;
            } catch (ParseException ignored) { }
        }

        return date;

    }
    public String assignShipType(int typeNum){

        String type = "";

        if (typeNum == 35){
            return type = "army";
        }
        if (typeNum >= 60 && typeNum <= 69){
            return type = "passenger transport";
        }
        if (typeNum >= 70 && typeNum <= 79){
            return type = "cargo";
        }
        else{
            return type = "others";
        }

    }


    public String calculateCell(double lat, double lon){

        String cell = "";

        double lowerLat = 32.0;
        double upperLat = 45.0;
        double lowerLon = -6.0;
        double upperLon = 37.0;


        if (lat >= lowerLat && lat<=upperLat && lon>=lowerLon && lon<=upperLon){

            List<Double> latSectors = new ArrayList<>();
            List<Double> lonSectors = new ArrayList<>();

            latSectors.add(lowerLat);
            lonSectors.add(lowerLon);

            double latRange = (upperLat-lowerLat);
            double lonRange = (upperLon-lowerLon);

            latRange = latRange / 10;

            double sumLat = lowerLat;

            for (int i=0;i<9;i++){
                sumLat = sumLat + latRange;
                latSectors.add(sumLat);
            }

            latSectors.add(upperLat);
            lonRange = lonRange / 40;

            double sumLon = lowerLon;

            for (int i=0;i<39;i++){
                sumLon = sumLon + lonRange;
                lonSectors.add(sumLon);
            }

            lonSectors.add(upperLon);

            //latitudine
            int indexLat = retrieveIndex(latSectors, lat);
            String latLetter = getCharFromNumber(indexLat);

            //longitudine
            int indexLon = retrieveIndex(lonSectors, lon);
            String indexLonStr = String.valueOf(indexLon);

            cell = latLetter.concat(indexLonStr);
            System.out.println("-- CELLA= "+cell);
            return cell;
        }

        return cell;

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

        for (int i=0;i<list.size();i++){

            if (lon > list.get(i) && lon < list.get(i+1)){
                res = i+1;
                break;
            }
        }
        return res;
    }

    public static String getCharFromNumber(int i){

        List<String> letterList = Arrays.asList(new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I","J"});
        String res = letterList.get(i-1);

        return res;



    }





    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    /*
    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

     */

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getCell() {
        return cell;
    }

    public void setCell(String cell) {
        this.cell = cell;
    }



    public Date getTsDate() {
        return tsDate;
    }

    public void setTsDate(Date tsDate) {
        this.tsDate = tsDate;
    }


    public Integer getShipTypeInt() {
        return shipTypeInt;
    }

    public void setShipTypeInt(Integer shipTypeInt) {
        this.shipTypeInt = shipTypeInt;
    }

    public String getShipType() {
        return shipType;
    }

    public void setShipType(String shipType) {
        this.shipType = shipType;
    }
}
