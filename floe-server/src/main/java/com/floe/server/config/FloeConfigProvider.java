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
