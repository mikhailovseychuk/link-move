package com.nhl.link.move.runtime.task.createorupdate;

import com.nhl.link.move.annotation.*;
import com.nhl.link.move.runtime.task.common.TaskStageType;

import java.lang.annotation.Annotation;

public enum CreateOrUpdateStage implements TaskStageType {
    EXTRACT_SOURCE_ROWS(AfterSourceRowsExtracted.class),
    CONVERT_SOURCE_ROWS(AfterSourceRowsConverted.class),
    MAP_SOURCE(AfterSourcesMapped.class),
    MATCH_TARGET(AfterTargetsMatched.class),
    MAP_TARGET(AfterTargetsMapped.class),
    RESOLVE_FK_VALUES(AfterFksResolved.class),
    MERGE_TARGET(AfterTargetsMerged.class),
    COMMIT_TARGET(AfterTargetsCommitted.class);


    private final Class<? extends Annotation> legacyAnnotation;

    CreateOrUpdateStage(Class<? extends Annotation> legacyAnnotation) {
        this.legacyAnnotation = legacyAnnotation;
    }

    @Override
    public Class<? extends Annotation> getLegacyAnnotation() {
        return legacyAnnotation;
    }
}
