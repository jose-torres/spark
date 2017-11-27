package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.sources.v2.BaseStreamingSource;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Optional;

public interface ContinuousReader extends BaseStreamingSource, DataSourceV2Reader {
    /**
     * Merges offsets coming from each partition into a single global offset.
     *
     * @param offsets
     */
    Offset mergeOffsets(Offset[] offsets);

    /**
     * Returns a list of read tasks. Each task is responsible for outputting data for one RDD
     * partition. That means the number of tasks returned here is same as the number of RDD
     * partitions this scan outputs.
     *
     * Note that, this may not be a full scan if the data source reader mixes in other optimization
     * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
     * Spark issues the scan request.
     *
     * If this method fails (by throwing an exception), the action would fail and no Spark job was
     * submitted.
     */
    List<ReadTask<Row>> createReadTasks(Optional<Offset> startOffset);

    @Override
    default List<ReadTask<Row>> createReadTasks() {
        throw new IllegalStateException(
            "createReadTasks with no offset should not be called for ContinuousReader.");
    }
}
