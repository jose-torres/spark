package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.execution.streaming.Offset;

public interface ContinuousDataReader<T> extends DataReader<T> {
    Offset getOffset();

    void outputMarker();
}
