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

package com.floe.server.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.auth.Permission;
import jakarta.ws.rs.NameBinding;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Test;

class SecuredTest {

    @Secured(Permission.READ_POLICIES)
    static class SinglePermissionClass {}

    @Secured({Permission.READ_POLICIES, Permission.WRITE_POLICIES})
    static class MultiplePermissionsClass {}

    @Secured(Permission.MANAGE_API_KEYS)
    public void singlePermissionMethod() {}

    @Secured({Permission.TRIGGER_MAINTENANCE, Permission.READ_POLICIES})
    public void multiplePermissionsMethod() {}

    @Test
    void shouldBeAnnotatedWithNameBinding() {
        assertTrue(Secured.class.isAnnotationPresent(NameBinding.class));
    }

    @Test
    void shouldHaveRuntimeRetention() {
        Retention retention = Secured.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    void shouldTargetTypeAndMethod() {
        Target target = Secured.class.getAnnotation(Target.class);
        assertNotNull(target);
        ElementType[] types = target.value();
        assertEquals(2, types.length);
        assertTrue(containsElementType(types, ElementType.TYPE));
        assertTrue(containsElementType(types, ElementType.METHOD));
    }

    @Test
    void shouldExtractSinglePermissionFromClass() {
        Secured secured = SinglePermissionClass.class.getAnnotation(Secured.class);
        assertNotNull(secured);
        assertEquals(1, secured.value().length);
        assertEquals(Permission.READ_POLICIES, secured.value()[0]);
    }

    @Test
    void shouldExtractMultiplePermissionsFromClass() {
        Secured secured = MultiplePermissionsClass.class.getAnnotation(Secured.class);
        assertNotNull(secured);
        assertEquals(2, secured.value().length);
        assertEquals(Permission.READ_POLICIES, secured.value()[0]);
        assertEquals(Permission.WRITE_POLICIES, secured.value()[1]);
    }

    @Test
    void shouldExtractSinglePermissionFromMethod() throws NoSuchMethodException {
        Secured secured =
                SecuredTest.class
                        .getDeclaredMethod("singlePermissionMethod")
                        .getAnnotation(Secured.class);
        assertNotNull(secured);
        assertEquals(1, secured.value().length);
        assertEquals(Permission.MANAGE_API_KEYS, secured.value()[0]);
    }

    @Test
    void shouldExtractMultiplePermissionsFromMethod() throws NoSuchMethodException {
        Secured secured =
                SecuredTest.class
                        .getDeclaredMethod("multiplePermissionsMethod")
                        .getAnnotation(Secured.class);
        assertNotNull(secured);
        assertEquals(2, secured.value().length);
        assertEquals(Permission.TRIGGER_MAINTENANCE, secured.value()[0]);
        assertEquals(Permission.READ_POLICIES, secured.value()[1]);
    }

    private boolean containsElementType(ElementType[] types, ElementType target) {
        for (ElementType type : types) {
            if (type == target) {
                return true;
            }
        }
        return false;
    }
}
