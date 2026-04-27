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

package com.floe.server.config;

import io.smallrye.config.SmallRyeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;

/** CDI wrapper for FloeConfig that can be injected into @Provider classes. */
@ApplicationScoped
public class FloeConfigProvider {

    @Inject Config config;

    private volatile FloeConfig floeConfig;

    /** Gets the FloeConfig instance lazily from SmallRyeConfig */
    public FloeConfig get() {
        if (floeConfig == null) {
            synchronized (this) {
                if (floeConfig == null) {
                    SmallRyeConfig smallRyeConfig = config.unwrap(SmallRyeConfig.class);
                    if (smallRyeConfig == null) {
                        throw new IllegalStateException(
                                "Failed to unwrap SmallRyeConfig from Config");
                    }
                    floeConfig = smallRyeConfig.getConfigMapping(FloeConfig.class);
                }
            }
        }
        return floeConfig;
    }

    // Convenience methods for common config access
    public boolean isAuthEnabled() {
        return get().auth().enabled();
    }

    public String authHeaderName() {
        return get().auth().headerName();
    }

    public FloeConfig.Auth auth() {
        return get().auth();
    }

    public FloeConfig.Security security() {
        return get().security();
    }
}
