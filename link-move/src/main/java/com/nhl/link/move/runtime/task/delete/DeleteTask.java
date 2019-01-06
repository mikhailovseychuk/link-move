package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.batch.BatchProcessor;
import com.nhl.link.move.batch.BatchRunner;
import com.nhl.link.move.df.DataFrame;
import com.nhl.link.move.df.DataRow;
import com.nhl.link.move.df.Index;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.task.BaseTask;
import com.nhl.link.move.runtime.token.ITokenManager;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.ResultIterator;
import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.query.ObjectSelect;

import java.util.Map;

/**
 * A task that allows to delete target objects not present in the source.
 *
 * @since 1.3
 */
public class DeleteTask<T extends DataObject> extends BaseTask {

    String extractorName;
    int batchSize;
    Class<T> type;
    Expression targetFilter;

    private DeleteSegmentProcessor<T> processor;
    private ITargetCayenneService targetCayenneService;

    public DeleteTask(
            String extractorName,
            int batchSize,
            Class<T> type,
            Expression targetFilter,
            ITargetCayenneService targetCayenneService,
            ITokenManager tokenManager,
            DeleteSegmentProcessor<T> processor) {

        super(tokenManager);

        this.extractorName = extractorName;
        this.batchSize = batchSize;
        this.type = type;
        this.targetFilter = targetFilter;
        this.targetCayenneService = targetCayenneService;
        this.processor = processor;
    }

    @Override
    public Execution run(Map<String, ?> params) {

        try (Execution execution = new Execution("DeleteTask:" + extractorName, params);) {

            BatchProcessor<T> batchProcessor = createBatchProcessor(execution);

            try (ResultIterator<T> data = createTargetSelect()) {
                BatchRunner.create(batchProcessor).withBatchSize(batchSize).run(data);
            }

            return execution;
        }
    }

    protected ResultIterator<T> createTargetSelect() {
        ObjectSelect<T> query = ObjectSelect.query(type).where(targetFilter);
        return targetCayenneService.newContext().iterator(query);
    }

    protected BatchProcessor<T> createBatchProcessor(Execution execution) {

        Index columns = Index.withNames(DeleteSegment.TARGET_COLUMN);

        return rows -> {

            // executing in the select context..
            ObjectContext context = rows.get(0).getObjectContext();

            // TODO: Guess we need something like a Series object next to the DataFrame to handle single column data
            DataFrame df = DataFrame.create(columns, rows, t -> DataRow.row(t));
            processor.process(execution, new DeleteSegment<T>(context, df));
        };
    }
}
