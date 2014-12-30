package io.praesid.livestats;

import lombok.ToString;

@ToString
public class DecayedStats extends Stats {
    public final int decays;
    public final double decayedN;
    public final double decayedMin;
    public final double decayedMax;

    public DecayedStats(final String name, final DecayingLiveStats stats) {
        super(name, stats);
        decayedN = stats.decayedNum();
        decays = stats.decayCount();
        decayedMin = stats.decayedMinimum();
        decayedMax = stats.decayedMaximum();
    }
}
