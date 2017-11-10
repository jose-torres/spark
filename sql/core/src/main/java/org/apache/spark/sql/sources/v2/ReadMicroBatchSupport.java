package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.Offset;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide streaming microbatch data reading ability.
 */
@InterfaceStability.Evolving
public interface ReadMicroBatchSupport extends ReadSupport, BaseStreamingSource {
  /**
   * Set the current offset range. A DataSourceV2Reader this source creates will be restricted to the
   * set range.
   *
   * @param start The beginning of the range. If absent, all available data up to end will be read.
   * @param end The end of the range.
   */
  void setOffsetRange(Optional<Offset> start, Offset end);

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  Optional<Offset> getOffset();

}
