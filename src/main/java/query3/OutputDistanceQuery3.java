package query3;

import java.util.Date;

public class OutputDistanceQuery3 {

    private String tripId;
    private Date date;
    private Double distance;

    public OutputDistanceQuery3(Double result){
        this.distance = result;

    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "OutputDistanceQuery3{" +
                "tripId='" + tripId + '\'' +
                ", date=" + date +
                ", distance=" + distance +
                '}';
    }
}
