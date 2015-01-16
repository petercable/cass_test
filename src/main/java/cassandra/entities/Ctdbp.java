package cassandra.entities;

import info.archinnov.achilles.annotations.*;

import java.util.List;

@Entity(table="ctdbp")
public class Ctdbp {

    @CompoundPrimaryKey
    private ReferenceDesignator refdesig;
    @Column
    private double driver_timestamp;
    @Column
    private double ingestion_timestamp;
    @Column
    private double internal_timestamp;
    @Column
    private String preferred_timestamp;
    @Column
    private int conductivity;
    @Column
    private int ctd_time;
    @Column
    private int pressure;
    @Column
    private int pressure_temp;
    @Column
    private int temperature;

    public ReferenceDesignator getRefdesig() {
        return refdesig;
    }

    public void setRefdesig(ReferenceDesignator refdesig) {
        this.refdesig = refdesig;
    }

    public int getConductivity() {
        return conductivity;
    }

    public void setConductivity(int conductivity) {
        this.conductivity = conductivity;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getPressure_temp() {
        return pressure_temp;
    }

    public void setPressure_temp(int pressure_temp) {
        this.pressure_temp = pressure_temp;
    }

    public int getPressure() {
        return pressure;
    }

    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    public int getCtd_time() {
        return ctd_time;
    }

    public void setCtd_time(int ctd_time) {
        this.ctd_time = ctd_time;
    }

    public String getPreferred_timestamp() {
        return preferred_timestamp;
    }

    public void setPreferred_timestamp(String preferred_timestamp) {
        this.preferred_timestamp = preferred_timestamp;
    }

    public double getInternal_timestamp() {
        return internal_timestamp;
    }

    public void setInternal_timestamp(double internal_timestamp) {
        this.internal_timestamp = internal_timestamp;
    }

    public double getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(double ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }

    public double getDriver_timestamp() {
        return driver_timestamp;
    }

    public void setDriver_timestamp(double driver_timestamp) {
        this.driver_timestamp = driver_timestamp;
    }

    public static class ReferenceDesignator {
        @PartitionKey(1)
        private String subsite;

        @PartitionKey(2)
        private String node;

        @PartitionKey(3)
        private String sensor;

        @ClusteringColumn
        private long time;

        public ReferenceDesignator() {}

        public ReferenceDesignator(String subsite, String node, String sensor) {
            this.subsite = subsite;
            this.node = node;
            this.sensor = sensor;
        }

        public ReferenceDesignator(String subsite, String node, String sensor, long time) {
            this(subsite, node, sensor);
            this.time = time;
        }

        public ReferenceDesignator(String refdes) {
            String[] parts = refdes.split("-", 3);

            if (parts.length != 3)
            {
                throw new IllegalArgumentException("Input did not contain three parts.");
            }

            this.subsite = parts[0];
            this.node = parts[1];
            this.sensor = parts[2];
        }

        public ReferenceDesignator(String refdes, long time) {
            this(refdes);
            this.time = time;
        }

        public String getSubsite() {
            return subsite;
        }

        public void setSubsite(String subsite) {
            this.subsite = subsite;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public String getSensor() {
            return sensor;
        }

        public void setSensor(String sensor) {
            this.sensor = sensor;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }
}
