package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.Execution;
import com.nhl.link.move.runtime.task.common.CallbackHolder;

public class DeleteSegmentProcessor {

    private final TargetMapper targetMapper;
    private final MissingTargetsFilterStage missingTargetsFilter;
    private final DeleteTargetStage deleter;
    private final CallbackHolder<DeleteSegment, DeleteStage> callbackHolder;

    public DeleteSegmentProcessor(
            TargetMapper targetMapper,
            MissingTargetsFilterStage missingTargetsFilter,
            DeleteTargetStage deleter,
            CallbackHolder<DeleteSegment, DeleteStage> callbackHolder) {
        this.targetMapper = targetMapper;
        this.missingTargetsFilter = missingTargetsFilter;
        this.deleter = deleter;
        this.callbackHolder = callbackHolder;
    }

    public void process(Execution exec, DeleteSegment segment) {
        callbackHolder.executeCallbacks(DeleteStage.EXTRACT_TARGET, exec, segment);

        mapTarget(exec, segment);
        filterMissingTargets(exec, segment);
        deleteTarget(segment);
        commitTarget(exec, segment);
    }

    private void mapTarget(Execution exec, DeleteSegment segment) {
        segment.setMappedTargets(targetMapper.map(segment.getTargets()));
        callbackHolder.executeCallbacks(DeleteStage.MAP_TARGET, exec, segment);
    }

    private void filterMissingTargets(Execution exec, DeleteSegment segment) {
        segment.setMissingTargets(missingTargetsFilter.filterMissing(
                segment.getMappedTargets(),
                segment.getSourceKeys())
        );
        callbackHolder.executeCallbacks(DeleteStage.FILTER_MISSING_TARGETS, exec, segment);
    }

    private void deleteTarget(DeleteSegment segment) {
        deleter.delete(segment.getContext(), segment.getMissingTargets());
    }

    private void commitTarget(Execution exec, DeleteSegment segment) {
        segment.getContext().commitChanges();
        callbackHolder.executeCallbacks(DeleteStage.COMMIT_TARGET, exec, segment);
    }
}
