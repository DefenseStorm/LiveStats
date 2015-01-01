package io.praesid.livestats;

import lombok.ToString;

import java.time.Duration;

@ToString
public class DecayConfig {

    public static final DecayConfig NEVER = new DecayConfig(1, Duration.ofNanos(0)) {
        @Override
        public String toString() {
            return "NEVER";
        }
    };

    public final double multiplier;
    public final long period;

    public DecayConfig(final double multiplier, final Duration period) {
        this.multiplier = multiplier;
        this.period = period.toNanos();
    }
}
