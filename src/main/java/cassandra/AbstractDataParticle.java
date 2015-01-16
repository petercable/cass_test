package cassandra;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public abstract class AbstractDataParticle {
    @Transient
    public static final long NTP_UNIX_DELTA_SECONDS = 2208988800l;
    public static long lasttime = 0;

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

    public void parseMap(Map<String, Object> map, String sensor) throws Exception {
        refdesig = sensor;

        if (map.get(preferred_timestamp) != null) {
            preferred_timestamp = (String) map.remove("preferred_timestamp");
            Object preferred = map.get(preferred_timestamp);
            if (preferred == null) {
                throw new Exception("Unable to retrieve preferred timestamp");
            }
            time = (long) ((double)preferred * 1000000000);
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeInMillis(sensorNTPToUnix(time));
            year = cal.get(Calendar.YEAR);
            jday = cal.get(Calendar.DAY_OF_YEAR);
        } else {
            throw new Exception("Unable to determine preferred timestamp");
        }

        if (map.get("driver_timestamp") != null)
            driver_timestamp = (double) map.remove("driver_timestamp");
        if (map.get("ingestion_timestamp") != null)
            ingestion_timestamp = (double) map.remove("ingestion_timestamp");
        if (map.get("internal_timestamp") != null)
            internal_timestamp = (double) map.remove("internal_timestamp");
    }

    public void parseJson(String json, String sensor) {

    }

    public void fill() {
        GregorianCalendar cal = new GregorianCalendar();
        year = cal.get(Calendar.YEAR);
        jday = cal.get(Calendar.DAY_OF_YEAR);
        refdesig = "TEST";

        time = lasttime;
        while (time == lasttime)
            time = System.nanoTime();
        lasttime = time;
        preferred_timestamp = "internal_timestamp";
    }

    public abstract String toJson();

    private long sensorNTPToUnix(double timestamp) {
        return Math.round((timestamp - NTP_UNIX_DELTA_SECONDS) * 1000);
    }

    public String getRefdesig() {
        return refdesig;
    }

    public void setRefdesig(String refdesig) {
        this.refdesig = refdesig;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getJday() {
        return jday;
    }

    public void setJday(int jday) {
        this.jday = jday;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getDriver_timestamp() {
        return driver_timestamp;
    }

    public void setDriver_timestamp(double driver_timestamp) {
        this.driver_timestamp = driver_timestamp;
    }

    public double getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(double ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }

    public double getInternal_timestamp() {
        return internal_timestamp;
    }

    public void setInternal_timestamp(double internal_timestamp) {
        this.internal_timestamp = internal_timestamp;
    }

    public String getPreferred_timestamp() {
        return preferred_timestamp;
    }

    public void setPreferred_timestamp(String preferred_timestamp) {
        this.preferred_timestamp = preferred_timestamp;
    }
}
