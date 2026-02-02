package com.floe.core.policy;

import static org.assertj.core.api.Assertions.assertThat;

import com.floe.core.health.HealthThresholds;
import org.junit.jupiter.api.Test;

class MaintenancePolicyThresholdsTest {

    @Test
    void policyHasHealthThresholdsField() {
        HealthThresholds thresholds =
                HealthThresholds.builder().smallFilePercentWarning(5.0).build();
        MaintenancePolicy policy =
                MaintenancePolicy.builder().name("policy").healthThresholds(thresholds).build();

        assertThat(policy.healthThresholds()).isEqualTo(thresholds);
    }

    @Test
    void nullThresholdsUsesDefaults() {
        MaintenancePolicy policy = MaintenancePolicy.builder().name("policy").build();

        assertThat(policy.effectiveThresholds()).isEqualTo(HealthThresholds.defaults());
    }

    @Test
    void customThresholdsApplied() {
        HealthThresholds thresholds =
                HealthThresholds.builder().smallFilePercentWarning(12.0).build();
        MaintenancePolicy policy =
                MaintenancePolicy.builder().name("policy").healthThresholds(thresholds).build();

        assertThat(policy.effectiveThresholds().smallFilePercentWarning()).isEqualTo(12.0);
    }

    @Test
    void builderSupportsThresholds() {
        HealthThresholds thresholds =
                HealthThresholds.builder().smallFilePercentWarning(8.0).build();
        MaintenancePolicy policy =
                MaintenancePolicy.builder().name("policy").healthThresholds(thresholds).build();

        assertThat(policy.healthThresholds()).isSameAs(thresholds);
    }
}
