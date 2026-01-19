package com.floe.server.config;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import java.util.Collections;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * Factory for creating the FloeConfigSource.
 *
 * <p>This factory is discovered via Java ServiceLoader and creates the FloeConfigSource that reads
 * the external floe-config.yaml file.
 */
public class FloeConfigSourceFactory implements ConfigSourceFactory {

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        return Collections.singletonList(new FloeConfigSource());
    }
}
