package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

public interface ContinuousReadSupport extends BaseStreamingSource {
  /**
   * Creates a {@link DataSourceV2Reader} to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param offset the starting offset to read from.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  DataSourceV2Reader createContinuousReader(Optional<Offset> offset, Optional<StructType> schema, String metadataPath, DataSourceV2Options options);
}
