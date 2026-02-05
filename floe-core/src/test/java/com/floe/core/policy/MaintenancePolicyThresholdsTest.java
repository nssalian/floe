/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
