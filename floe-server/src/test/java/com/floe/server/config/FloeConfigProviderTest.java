package com.floe.server.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.smallrye.config.SmallRyeConfig;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FloeConfigProviderTest {

    @Mock private Config config;

    @Mock private SmallRyeConfig smallRyeConfig;

    @Mock private FloeConfig floeConfig;

    @Mock private FloeConfig.Auth authConfig;

    @Mock private FloeConfig.Security securityConfig;

    private FloeConfigProvider provider;

    @BeforeEach
    void setUp() {
        provider = new FloeConfigProvider();
        // Inject mocked config using reflection
        try {
            var configField = FloeConfigProvider.class.getDeclaredField("config");
            configField.setAccessible(true);
            configField.set(provider, config);
        } catch (Exception e) {
            fail("Could not inject config: " + e.getMessage());
        }

        // Only set up the common stubs - floeConfig.auth/security
        // are stubbed in each test as needed
        when(config.unwrap(SmallRyeConfig.class)).thenReturn(smallRyeConfig);
        when(smallRyeConfig.getConfigMapping(FloeConfig.class)).thenReturn(floeConfig);
    }

    @Test
    void getShouldReturnFloeConfig() {
        FloeConfig result = provider.get();

        assertNotNull(result);
        assertSame(floeConfig, result);
    }

    @Test
    void getShouldCacheFloeConfig() {
        FloeConfig result1 = provider.get();
        FloeConfig result2 = provider.get();

        assertSame(result1, result2);
        verify(config, times(1)).unwrap(SmallRyeConfig.class);
    }

    @Test
    void isAuthEnabledShouldReturnAuthEnabledStatus() {
        when(floeConfig.auth()).thenReturn(authConfig);
        when(authConfig.enabled()).thenReturn(true);

        assertTrue(provider.isAuthEnabled());
    }

    @Test
    void isAuthEnabledShouldReturnFalseWhenDisabled() {
        when(floeConfig.auth()).thenReturn(authConfig);
        when(authConfig.enabled()).thenReturn(false);

        assertFalse(provider.isAuthEnabled());
    }

    @Test
    void authHeaderNameShouldReturnHeaderName() {
        when(floeConfig.auth()).thenReturn(authConfig);
        when(authConfig.headerName()).thenReturn("X-API-Key");

        assertEquals("X-API-Key", provider.authHeaderName());
    }

    @Test
    void authShouldReturnAuthConfig() {
        when(floeConfig.auth()).thenReturn(authConfig);

        FloeConfig.Auth result = provider.auth();

        assertSame(authConfig, result);
    }

    @Test
    void securityShouldReturnSecurityConfig() {
        when(floeConfig.security()).thenReturn(securityConfig);

        FloeConfig.Security result = provider.security();

        assertSame(securityConfig, result);
    }
}
