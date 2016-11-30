package main.java;

import java.sql.*;

/**
 * Harish Kumar K V
 */
public class SensorParameters {
    private String latitude;
    private String longitude;
    private String city;
    private Date timestamp;
    private double temperature;
    private int CO2;
    private int CO;
    private int NO;
    private int O3;
    private double humidity;
    private double precipitation;
    private double windDirection;
    private double windSpeed;
    public String getLatitude() {
        return latitude;
    }
    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }
    public String getLongitude() {
        return longitude;
    }
    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public Date getTimestamp () {
        return timestamp;
    }
    public void setTimestamp (Date timestamp) {
        this.timestamp = timestamp;
    }
    public double getTemperature() {
        return temperature;
    }
    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
    public int getCO2 () {
        return CO2;
    }
    public void setCO2 (int CO2) {
        this.CO2 = CO2;
    }
    public int getCO () {
        return CO;
    }
    public void setCO (int CO) {
        this.CO = CO;
    }
    public int getNO () {
        return NO;
    }
    public void setNO (int NO) {
        this.NO = NO;
    }
    public int getO3() {
        return O3;
    }
    public void setO3(int o3) {
        this.O3 = o3;
    }
    public double getHumidity() {
        return humidity;
    }
    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }
    public double getPrecipitation() {
        return precipitation;
    }
    public void setPrecipitation(double precipitation) {
        this.precipitation = precipitation;
    }
    public double getWindDirection() {
        return windDirection;
    }
    public void setWindDirection(double windDirection) {
        this.windDirection = windDirection;
    }
    public double getWindSpeed() {
        return windSpeed;
    }
    public void setWindSpeed(double windSpeed) {
        this.windSpeed = windSpeed;
    }
}
