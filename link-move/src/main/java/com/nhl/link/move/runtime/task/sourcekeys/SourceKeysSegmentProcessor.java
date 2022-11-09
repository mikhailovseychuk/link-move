package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackHolder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;

import java.util.Set;

/**
 * @since 1.3
 */
public class SourceKeysSegmentProcessor {

    private final CallbackHolder<SourceKeysSegment, SourceKeysStage> callbackHolder;
    private final RowConverter rowConverter;
    private final SourceKeysCollector mapper;

    public SourceKeysSegmentProcessor(
            RowConverter rowConverter,
            SourceKeysCollector mapper,
            CallbackHolder<SourceKeysSegment, SourceKeysStage> callbackHolder) {
        this.rowConverter = rowConverter;
        this.mapper = mapper;
        this.callbackHolder = callbackHolder;
    }

    public void process(Execution exec, SourceKeysSegment segment) {
        callbackHolder.executeCallbacks(SourceKeysStage.EXTRACT_SOURCE_ROWS, exec, segment);

        convertSrc(segment);
        collectSourceKeys(exec, segment);
    }

    private void convertSrc(SourceKeysSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
    }

    private void collectSourceKeys(Execution exec, SourceKeysSegment segment) {

        @SuppressWarnings("unchecked")
        Set<Object> keys = (Set<Object>) exec.getAttribute(SourceKeysTask.RESULT_KEY);
        mapper.collectSourceKeys(keys, segment.getSources());

        callbackHolder.executeCallbacks(SourceKeysStage.COLLECT_SOURCE_KEYS, exec, segment);
    }
}
