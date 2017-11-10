package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide streaming microbatch data reading ability.
 */
@InterfaceStability.Evolving
public interface ReadMicroBatchSupport extends BaseStreamingSource {
  /**
   * Creates a {@link DataSourceV2Reader} to scan a batch of data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   *
   * @param start The beginning of the batch. If absent, all available data up to end will be read.
   * @param end The end of the batch.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  DataSourceV2Reader createReader(Optional<Offset> start, Offset end, DataSourceV2Options options);

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  Optional<Offset> getOffset();

}
