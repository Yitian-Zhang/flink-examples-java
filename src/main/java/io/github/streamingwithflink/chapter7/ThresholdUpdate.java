package io.github.streamingwithflink.chapter7;

public class ThresholdUpdate {

    private String sensorId;

    private Double threshold;

    public ThresholdUpdate() {}

    public ThresholdUpdate(String sensorId, Double threshold) {
        this.sensorId = sensorId;
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "ThresholdUpdate{" +
                "sensorId='" + sensorId + '\'' +
                ", threshold=" + threshold +
                '}';
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }
}
