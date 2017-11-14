package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;

@InterfaceStability.Evolving
public interface ContinuousWriter extends DataSourceV2Writer {
  void commit(long epochId, WriterCommitMessage[] messages);

  String getQueryId();
}
