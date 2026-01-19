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
