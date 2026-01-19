package com.floe.server.ui;

import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.TablePattern;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for policies pages. */
@Path("/ui/policies")
@Produces(MediaType.TEXT_HTML)
public class PoliciesUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(PoliciesUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm a").withZone(ZoneId.systemDefault());

    @Inject Template policies;

    @Inject
    @io.quarkus.qute.Location("policy-detail.html")
    Template policyDetail;

    @Inject PolicyStore policyStore;

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;

    /** Main policies list page with pagination. */
    @GET
    public TemplateInstance index(
            @QueryParam("limit") @DefaultValue("50") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        LOG.debug("Loading policies list: limit={}, offset={}", limit, offset);

        // Clamp limit to valid range
        limit = Math.max(1, Math.min(limit, MAX_LIMIT));
        offset = Math.max(0, offset);

        List<MaintenancePolicy> allPolicies = policyStore.listAll();
        int totalCount = allPolicies.size();

        // Apply pagination
        List<MaintenancePolicy> paginatedPolicies =
                allPolicies.stream().skip(offset).limit(limit).toList();

        List<PolicyListView> views = paginatedPolicies.stream().map(PolicyListView::from).toList();

        long enabledCount =
                allPolicies.stream().filter(p -> Boolean.TRUE.equals(p.enabled())).count();

        boolean hasMore = offset + views.size() < totalCount;

        return page(policies, "/ui/policies")
                .data("policies", views)
                .data("totalCount", totalCount)
                .data("enabledCount", enabledCount)
                .data("disabledCount", totalCount - enabledCount)
                .data("limit", limit)
                .data("offset", offset)
                .data("hasMore", hasMore)
                .data("nextOffset", offset + limit)
                .data("prevOffset", Math.max(0, offset - limit))
                .data("hasPrev", offset > 0);
    }

    /** Policy detail page. */
    @GET
    @Path("/{id}")
    public Response detail(@PathParam("id") String id) {
        LOG.debug("Viewing policy: {}", id);

        Optional<MaintenancePolicy> policyOpt = policyStore.getById(id);

        if (policyOpt.isEmpty()) {
            return Response.seeOther(URI.create("/ui/policies")).build();
        }

        PolicyDetailView view = PolicyDetailView.from(policyOpt.get());
        TemplateInstance template = page(policyDetail, "/ui/policies").data("policy", view);

        return Response.ok(template).build();
    }

    // View models for templates

    /** View model for policies list. */
    public record PolicyListView(
            String id,
            String name,
            String description,
            String tablePatternDisplay,
            boolean enabled,
            int priority,
            boolean hasRewriteDataFiles,
            boolean hasExpireSnapshots,
            boolean hasOrphanCleanup,
            boolean hasRewriteManifests,
            boolean hasAnyOperations) {
        public static PolicyListView from(MaintenancePolicy policy) {
            return new PolicyListView(
                    policy.id(),
                    policy.getNameOrDefault(),
                    policy.description(),
                    formatTablePattern(policy.tablePattern()),
                    Boolean.TRUE.equals(policy.enabled()),
                    policy.priority(),
                    policy.rewriteDataFiles() != null,
                    policy.expireSnapshots() != null,
                    policy.orphanCleanup() != null,
                    policy.rewriteManifests() != null,
                    policy.hasAnyOperations());
        }
    }

    /** View model for policy detail page. */
    public record PolicyDetailView(
            String id,
            String name,
            String description,
            String tablePatternDisplay,
            boolean enabled,
            int priority,
            boolean hasRewriteDataFiles,
            boolean hasExpireSnapshots,
            boolean hasOrphanCleanup,
            boolean hasRewriteManifests,
            int operationCount,
            String createdAtFormatted,
            String updatedAtFormatted,
            boolean hasTags,
            List<TagView> tags) {
        public static PolicyDetailView from(MaintenancePolicy policy) {
            int opCount = 0;
            if (policy.rewriteDataFiles() != null) opCount++;
            if (policy.expireSnapshots() != null) opCount++;
            if (policy.orphanCleanup() != null) opCount++;
            if (policy.rewriteManifests() != null) opCount++;

            List<TagView> tagViews = List.of();
            if (policy.tags() != null && !policy.tags().isEmpty()) {
                tagViews =
                        policy.tags().entrySet().stream()
                                .map(e -> new TagView(e.getKey(), e.getValue()))
                                .toList();
            }

            return new PolicyDetailView(
                    policy.id(),
                    policy.getNameOrDefault(),
                    policy.description(),
                    formatTablePattern(policy.tablePattern()),
                    Boolean.TRUE.equals(policy.enabled()),
                    policy.priority(),
                    policy.rewriteDataFiles() != null,
                    policy.expireSnapshots() != null,
                    policy.orphanCleanup() != null,
                    policy.rewriteManifests() != null,
                    opCount,
                    formatInstant(policy.createdAt()),
                    formatInstant(policy.updatedAt()),
                    policy.tags() != null && !policy.tags().isEmpty(),
                    tagViews);
        }
    }

    /**
     * View model for tags.
     *
     * @param key tag key
     * @param value tag value
     */
    public record TagView(String key, String value) {}

    // Formatting helpers

    private static String formatTablePattern(TablePattern pattern) {
        if (pattern == null) {
            return "*.*.*";
        }
        return pattern.toString();
    }

    private static String formatInstant(Instant instant) {
        if (instant == null || instant.equals(Instant.EPOCH)) {
            return "-";
        }
        return DATE_FORMAT.format(instant);
    }
}
