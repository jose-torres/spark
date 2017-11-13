package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;

@InterfaceStability.Evolving
public interface SupportsContinuousWrite {
  void commit(long epochId, WriterCommitMessage[] messages);
}
