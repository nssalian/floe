package com.floe.server.auth;

import com.floe.core.auth.Permission;
import jakarta.ws.rs.NameBinding;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to secure JAX-RS endpoints with permission-based access control.
 *
 * <p>When applied to a resource class or method, the request must be authenticated and have at
 * least one of the specified permissions.
 */
@NameBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Secured {
    /** The permissions required to access the endpoint (user must have at least one). */
    Permission[] value();
}
