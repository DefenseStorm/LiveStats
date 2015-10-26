package io.praesid.livestats;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.Duration;
import java.util.Objects;

@ToString
@EqualsAndHashCode
public final class DecayConfig {
    public static final DecayConfig NEVER = new DecayConfig();

    public final double multiplier;
    public final long period;

    /**
     * DecayConfig constructor with no period. This is best used when you want to decay values exponentially over time
     * and the values will never be "invalid". A typical value for "multiplier" is 0.95.
     *
     * @param multiplier The exponential moving average coefficient
     */
    public DecayConfig(final double multiplier) {
        Preconditions.checkArgument(multiplier >= 0, "Multiplier must be >= 0");
        Preconditions.checkArgument(multiplier < 1, "Multiplier must be < 1");
        this.multiplier = multiplier;
        this.period = 0;
    }

    /**
     * DecayConfig constructor with a period. This is best used when the data has a strict time bound, where it would
     * not make sense to keep values around forever. A typical value for "multiplier" is 0.95 and the period depends
     * on how long the data is valid.
     *
     * @param multiplier The exponential moving average coefficient
     * @param period The time period where the data is valid
     */
    public DecayConfig(final double multiplier, final Duration period) {
        Preconditions.checkArgument(multiplier >= 0, "Multiplier must be >= 0");
        Preconditions.checkArgument(multiplier < 1, "Multiplier must be < 1");
        Preconditions.checkNotNull(period, "Period must be specified");
        Preconditions.checkArgument(!period.isNegative() && !period.isZero(), "Period must be positive");
        this.multiplier = multiplier;
        this.period = period.toNanos();
    }

    private DecayConfig() {
        multiplier = 1;
        period = 0;
    }
}
