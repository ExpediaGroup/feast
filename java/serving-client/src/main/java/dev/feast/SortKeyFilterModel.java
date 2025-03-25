package dev.feast;

import feast.proto.serving.ServingAPIProto.SortKeyFilter;
import feast.proto.types.ValueProto.Value;

public class SortKeyFilterModel {
    private String sortKeyName;
    private Value rangeStart;
    private Value rangeEnd;
    private boolean startInclusive;
    private boolean endInclusive;

    public SortKeyFilterModel(String sortKeyName, Object rangeStart, Object rangeEnd, boolean inclusiveStart, boolean inclusiveEnd) {
        this.sortKeyName = sortKeyName;
        this.rangeStart = RequestUtil.objectToValue(rangeStart);
        this.rangeEnd = RequestUtil.objectToValue(rangeEnd);
        this.startInclusive = inclusiveStart;
        this.endInclusive = inclusiveEnd;
    }

    public SortKeyFilter toProto() {
        return SortKeyFilter.newBuilder()
                .setSortKeyName(sortKeyName)
                .setRangeStart(rangeStart)
                .setRangeEnd(rangeEnd)
                .setStartInclusive(startInclusive)
                .setEndInclusive(endInclusive)
                .build();
    }

    public String getSortKeyName() {
        return sortKeyName;
    }

    public Value getRangeStart() {
        return rangeStart;
    }

    public Value getRangeEnd() {
        return rangeEnd;
    }

    public boolean isStartInclusive() {
        return startInclusive;
    }

    public boolean isEndInclusive() {
        return endInclusive;
    }
}
