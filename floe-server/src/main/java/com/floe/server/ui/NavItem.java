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

/**
 * Navigation item for the UI.
 *
 * @param label Display text
 * @param href Link target
 * @param active Whether this item is currently active
 */
public record NavItem(String label, String href, boolean active) {

    public static NavItem of(String label, String href) {
        return new NavItem(label, href, false);
    }

    public static NavItem active(String label, String href) {
        return new NavItem(label, href, true);
    }

    public NavItem withActive(boolean active) {
        return new NavItem(label, href, active);
    }
}
