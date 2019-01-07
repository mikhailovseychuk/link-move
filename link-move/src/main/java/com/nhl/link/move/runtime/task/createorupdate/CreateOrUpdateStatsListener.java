package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.ExecutionStats;
import com.nhl.link.move.annotation.AfterTargetsMerged;
import com.nhl.yadf.Index;

/**
 * A listener that collects task stats and stores them in the Execution's
 * {@link ExecutionStats} object.
 *
 * @since 1.3
 */
public class CreateOrUpdateStatsListener {

    private static final CreateOrUpdateStatsListener instance = new CreateOrUpdateStatsListener();

    public static CreateOrUpdateStatsListener instance() {
        return instance;
    }

    @AfterTargetsMerged
    public void targetCreated(Execution e, CreateOrUpdateSegment<?> segment) {
        ExecutionStats stats = e.getStats();
        segment.getMerged().consume((c, r) -> updateStats(stats, c, r));
    }

    private void updateStats(ExecutionStats stats, Index columns, Object[] row) {

        boolean created = (boolean) columns.get(row, CreateOrUpdateSegment.TARGET_CREATED_COLUMN);
        if (created) {
            stats.incrementCreated(1);
        } else {
            stats.incrementUpdated(1);
        }
    }
}
