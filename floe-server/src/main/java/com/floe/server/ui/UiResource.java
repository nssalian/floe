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

package com.floe.server.ui;

import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Base class for UI resources providing common template functionality.
 *
 * <p>Subclasses should inject their specific templates and use the helper methods to build
 * consistent page responses with navigation state.
 */
public abstract class UiResource {

    private static final List<NavItem> NAV_ITEMS =
            List.of(
                    NavItem.of("Dashboard", "/ui/"),
                    NavItem.of("Policies", "/ui/policies"),
                    NavItem.of("Operations", "/ui/operations"),
                    NavItem.of("Tables", "/ui/tables"),
                    NavItem.of("Settings", "/ui/settings"));

    @Inject Template alert;

    /**
     * Builds navigation items with the specified path marked as active.
     *
     * @param activePath the current page path (e.g., "/ui/policies")
     * @return list of NavItems with correct active state
     */
    protected List<NavItem> buildNavItems(String activePath) {
        return NAV_ITEMS.stream()
                .map(
                        item -> {
                            // Exact match for dashboard, prefix match for others
                            boolean isActive =
                                    item.href().equals("/ui/")
                                            ? activePath.equals("/ui/") || activePath.equals("/ui")
                                            : activePath.startsWith(item.href());
                            return item.withActive(isActive);
                        })
                .toList();
    }

    /**
     * Creates a page template instance with standard data.
     *
     * @param template the page template
     * @param activePath the current page path for nav highlighting
     * @return configured TemplateInstance
     */
    protected TemplateInstance page(Template template, String activePath) {
        return template.data("navItems", buildNavItems(activePath));
    }

    /**
     * Creates an error alert fragment.
     *
     * @param title error title
     * @param message error details
     * @return configured alert TemplateInstance
     */
    protected TemplateInstance errorAlert(String title, String message) {
        return alert.data("type", "error").data("title", title).data("message", message);
    }

    /**
     * Creates a success alert fragment.
     *
     * @param title success title
     * @param message success details
     * @return configured alert TemplateInstance
     */
    protected TemplateInstance successAlert(String title, String message) {
        return alert.data("type", "success").data("title", title).data("message", message);
    }

    /**
     * Creates an info alert fragment.
     *
     * @param message info message
     * @return configured alert TemplateInstance
     */
    protected TemplateInstance infoAlert(String message) {
        return alert.data("type", "info").data("message", message);
    }

    /**
     * Creates a warning alert fragment.
     *
     * @param title warning title
     * @param message warning details
     * @return configured alert TemplateInstance
     */
    protected TemplateInstance warningAlert(String title, String message) {
        return alert.data("type", "warning").data("title", title).data("message", message);
    }
}
