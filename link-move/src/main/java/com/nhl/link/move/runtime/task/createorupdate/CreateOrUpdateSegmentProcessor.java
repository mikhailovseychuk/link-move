package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackHolder;
import com.nhl.link.move.runtime.task.common.FkResolver;

/**
 * A stateless thread-safe processor for batch segments of a create-or-update ETL task.
 *
 * @since 1.3
 */
public class CreateOrUpdateSegmentProcessor {

    private final RowConverter rowConverter;
    private final SourceMapper sourceMapper;
    private final CreateOrUpdateTargetMatcher matcher;
    private final CreateOrUpdateTargetMapper mapper;
    private final CreateOrUpdateTargetMerger merger;
    private final FkResolver fkResolver;
    private final CallbackHolder<CreateOrUpdateSegment, CreateOrUpdateStage> callbackHolder;

    public CreateOrUpdateSegmentProcessor(
            RowConverter rowConverter,
            SourceMapper sourceMapper,
            CreateOrUpdateTargetMatcher matcher,
            CreateOrUpdateTargetMapper mapper,
            CreateOrUpdateTargetMerger merger,
            FkResolver fkResolver,
            CallbackHolder<CreateOrUpdateSegment, CreateOrUpdateStage> callbackHolder) {

        this.rowConverter = rowConverter;
        this.sourceMapper = sourceMapper;
        this.matcher = matcher;
        this.mapper = mapper;
        this.merger = merger;
        this.fkResolver = fkResolver;
        this.callbackHolder = callbackHolder;
    }

    public void process(Execution exec, CreateOrUpdateSegment segment) {

        callbackHolder.executeCallbacks(CreateOrUpdateStage.EXTRACT_SOURCE_ROWS, exec, segment);

        convertSrc(exec, segment);
        mapSrc(exec, segment);
        matchTarget(exec, segment);
        mapToTarget(exec, segment);
        resolveFks(exec, segment);
        mergeToTarget(exec, segment);
        commitTarget(exec, segment);
    }

    private void convertSrc(Execution exec, CreateOrUpdateSegment segment) {
        segment.setSources(rowConverter.convert(segment.getSourceRowsHeader(), segment.getSourceRows()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.CONVERT_SOURCE_ROWS, exec, segment);
    }

    private void mapSrc(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMappedSources(sourceMapper.map(segment.getSources()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.MAP_SOURCE, exec, segment);
    }

    private void matchTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMatchedTargets(matcher.match(segment.getContext(), segment.getMappedSources()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.MATCH_TARGET, exec, segment);
    }

    private void mapToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMapped(mapper.map(segment.getContext(), segment.getMappedSources(), segment.getMatchedTargets()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.MAP_TARGET, exec, segment);
    }

    private void resolveFks(Execution exec, CreateOrUpdateSegment segment) {
        segment.setFksResolved(fkResolver.resolveFks(segment.getContext(), segment.getMapped()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.RESOLVE_FK_VALUES, exec, segment);
    }

    private void mergeToTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.setMerged(merger.merge(segment.getFksResolved()));
        callbackHolder.executeCallbacks(CreateOrUpdateStage.MERGE_TARGET, exec, segment);
    }

    private void commitTarget(Execution exec, CreateOrUpdateSegment segment) {
        segment.getContext().commitChanges();
        callbackHolder.executeCallbacks(CreateOrUpdateStage.COMMIT_TARGET, exec, segment);
    }
}
