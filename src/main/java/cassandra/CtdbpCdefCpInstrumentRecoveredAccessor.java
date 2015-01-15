package cassandra;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
public interface CtdbpCdefCpInstrumentRecoveredAccessor {
    @Query("select * from ctdbp_cdef_cp_instrument_recovered")
    public Result<CtdbpCdefCpInstrumentRecovered> getAll();

    @Query("select * from ctdbp_cdef_cp_instrument_recovered")
    public ListenableFuture<Result<CtdbpCdefCpInstrumentRecovered>> getAllAsync();

    @Query("select * from ctdbp_cdef_cp_instrument_recovered where refdesig = :refdesig and year = :year and jday = :jday")
    public Result<CtdbpCdefCpInstrumentRecovered> getDay(@Param("refdesig") String refdesig,
                                                         @Param("year") int year,
                                                         @Param("jday") int jday);

    @Query("select * from ctdbp_cdef_cp_instrument_recovered where refdesig = :refdesig and year = :year and jday = :jday")
    public ListenableFuture<Result<CtdbpCdefCpInstrumentRecovered>> getDayAsync(@Param("refdesig") String refdesig,
                                                                                @Param("year") int year,
                                                                                @Param("jday") int jday);

    @Query("select distinct refdesig, year, jday from ctdbp_cdef_cp_instrument_recovered")
    public ResultSet getDistinct();
}
