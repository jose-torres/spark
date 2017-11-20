package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.execution.streaming.Offset;

public interface ContinuousReader {
    /**
     * Merges offsets coming from each partition into a single global offset.
     *
     * @param offsets
     */
    Offset mergeOffsets(Offset[] offsets);
}
