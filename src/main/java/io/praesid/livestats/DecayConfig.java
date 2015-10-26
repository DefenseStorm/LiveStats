package io.praesid.livestats;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.Duration;
import java.util.Objects;

@ToString
@EqualsAndHashCode
public class DecayConfig {
    public static final DecayConfig NEVER = new DecayConfig();

    public final double multiplier;
    public final long period;

    public DecayConfig(final double multiplier) {
        Preconditions.checkArgument(multiplier >= 0, "Multiplier must be >= 0");
        Preconditions.checkArgument(multiplier < 1, "Multiplier must be < 1");
        this.multiplier = multiplier;
        this.period = 0;
    }

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
