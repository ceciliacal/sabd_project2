package utils;

import java.util.Date;

public class DataEntity {

    String shipId;
    Integer shipType;
    double speed;
    double lon;
    double lat;
    String timestamp;
    String cell;
    Date tsDate;


    public DataEntity(String shipId, Integer shipType, double speed, double lon, double lat, String timestamp){
        this.shipId = shipId;
        this.shipType = shipType;
        this.speed = speed;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;



    }



    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

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

    public Integer getShipType() {
        return shipType;
    }

    public void setShipType(Integer shipType) {
        this.shipType = shipType;
    }

    public Date getTsDate() {
        return tsDate;
    }

    public void setTsDate(Date tsDate) {
        this.tsDate = tsDate;
    }
}
