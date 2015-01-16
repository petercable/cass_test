package cassandra;

import com.datastax.driver.mapping.annotations.*;

import java.util.Map;

@Table(keyspace = "ooi", name = "ctdbp_cdef_cp_instrument_recovered")
public class CtdbpCdefCpInstrumentRecovered extends AbstractDataParticle {
    @PartitionKey(0)
    protected String refdesig;
    @PartitionKey(1)
    protected int year;
    @PartitionKey(2)
    protected int jday;
    @ClusteringColumn
    protected long time;
    private double driver_timestamp;
    private double ingestion_timestamp;
    private double internal_timestamp;
    private String preferred_timestamp;
    private int conductivity;
    private int ctd_time;
    private int pressure;
    private int pressure_temp;
    private int temperature;

    public CtdbpCdefCpInstrumentRecovered() {}

    public CtdbpCdefCpInstrumentRecovered(Map<String, Object> map, String sensor) throws Exception {
        parseMap(map, sensor);
    }

    public void parseMap(Map<String, Object> map, String sensor) throws Exception {
        super.parseMap(map, sensor);
        if (map.get("conductivity") != null)
            conductivity = (int) map.remove("conductivity");
        if (map.get("ctd_time") != null)
            ctd_time = (int) map.remove("ctd_time");
        if (map.get("pressure") != null)
            pressure = (int) map.remove("pressure");
        if (map.get("pressure_temp") != null)
            pressure_temp = (int) map.remove("pressure_temp");
        if (map.get("temperature") != null)
            temperature = (int) map.remove("temperature");
    }

    public String toJson() {
        return "";
    }

    public int getConductivity() {
        return conductivity;
    }

    public void setConductivity(int conductivity) {
        this.conductivity = conductivity;
    }

    public int getCtd_time() {
        return ctd_time;
    }

    public void setCtd_time(int ctd_time) {
        this.ctd_time = ctd_time;
    }

    public int getPressure() {
        return pressure;
    }

    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    public int getPressure_temp() {
        return pressure_temp;
    }

    public void setPressure_temp(int pressure_temp) {
        this.pressure_temp = pressure_temp;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

}
