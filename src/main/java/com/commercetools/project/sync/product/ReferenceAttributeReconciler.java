package com.commercetools.project.sync.product;

import com.commercetools.api.client.ProjectApiRoot;
import com.commercetools.api.models.product.Attribute;
import com.commercetools.api.models.product.Product;
import com.commercetools.api.models.product.ProductProjection;
import com.commercetools.api.models.product.ProductPublishActionBuilder;
import com.commercetools.api.models.product.ProductSetAttributeActionBuilder;
import com.commercetools.api.models.product.ProductSetProductAttributeActionBuilder;
import com.commercetools.api.models.product.ProductUpdate;
import com.commercetools.api.models.product.ProductUpdateAction;
import com.commercetools.api.models.product.ProductUpdateBuilder;
import com.commercetools.api.models.product.ProductVariant;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vrap.rmf.base.client.ApiHttpException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step-2 reconciler for product-reference attributes.
 *
 * It reads the "truth" from SOURCE products (by key), resolves references into TARGET product IDs,
 * and updates TARGET products accordingly.
 *
 * Handles:
 * - Variant attributes (setAttribute with variantId)
 * - Product-level attributes (setProductAttribute)
 */
public final class ReferenceAttributeReconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReferenceAttributeReconciler.class);

    // PERF: fewer requests per rule; if your API max is lower, set back to 200.
    private static final int PAGE_SIZE = 500;

    private final ProjectApiRoot source;
    private final ProjectApiRoot target;
    private final ObjectMapper om = new ObjectMapper();

    private final List<Rule> rules = List.of(
            Rule.variantAttr("menu_product", "product_sku", 1L),
            Rule.productAttr("menu_sku", "sku_configurable_properties"),
            Rule.productAttr("configurable-properties", "configurationOptions"),
            Rule.productAttr("configurable-properties", "defaultSelectedConfigurationOption"),
            Rule.productAttr("configurable-option", "productRef"),
            Rule.productAttr("configurable-option", "dineInSku")
    );

    // Caches for speed
    private final Map<String, String> sourceIdToKey = new HashMap<>();
    private final Map<String, String> targetKeyToId = new HashMap<>();
    private final Map<String, String> productTypeKeyToId = new HashMap<>();

    public ReferenceAttributeReconciler(@Nonnull final ProjectApiRoot source, @Nonnull final ProjectApiRoot target) {
        this.source = source;
        this.target = target;
    }

    @Nonnull
    public CompletionStage<Void> run() {
        return loadProductTypeIdsViaGraphQl()
                .thenCompose(ignored -> reconcileAllRules())
                .thenAccept(ignored -> {});
    }

    @Nonnull
    private CompletionStage<Void> reconcileAllRules() {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Rule rule : rules) {
            chain = chain.thenCompose(ignored -> reconcileRule(rule).toCompletableFuture());
        }
        return chain;
    }

    @Nonnull
    private CompletionStage<Void> reconcileRule(@Nonnull final Rule rule) {
        LOGGER.info("Step-2: Reconciling rule {}", rule);

        final String ptId = productTypeKeyToId.get(rule.productTypeKey);
        if (ptId == null || ptId.isBlank()) {
            LOGGER.warn("Step-2: ProductType key '{}' not found in TARGET. Skipping rule {}.",
                    rule.productTypeKey, rule);
            return CompletableFuture.completedFuture(null);
        }

        return reconcileTargetProductsOfTypeId(ptId, null, rule);
    }

    @Nonnull
    private CompletionStage<Void> reconcileOneTargetProduct(@Nonnull final ProductProjection targetProjection, @Nonnull final Rule rule) {
        final String productKey = targetProjection.getKey();
        if (productKey == null || productKey.isBlank()) {
            return CompletableFuture.completedFuture(null);
        }

        // PERF: if target already has a resolved value, skip BEFORE fetching SOURCE.
        final Attribute targetAttr = rule.isVariant
                ? findVariantAttr(targetProjection.getMasterVariant(), rule.attributeName)
                : findProductAttr(targetProjection.getAttributes(), rule.attributeName);

        if (targetAttr != null && !attributeNeedsFix(targetAttr)) {
            return CompletableFuture.completedFuture(null);
        }

        // Fetch SOURCE by key (truth)
        return source.productProjections().withKey(productKey).get().withStaged(true).execute()
                .thenCompose(srcResp -> {
                    ProductProjection src = srcResp.getBody();
                    if (src == null) return CompletableFuture.completedFuture(null);

                    Attribute srcAttr = rule.isVariant
                            ? findVariantAttr(src.getMasterVariant(), rule.attributeName)
                            : findProductAttr(src.getAttributes(), rule.attributeName);

                    if (srcAttr == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    Object rewrittenValue = rewriteReferencesToTargetIds(srcAttr.getValue());
                    if (rewrittenValue == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // Otherwise apply (fill missing OR fix unresolved OR fix blank)
                    return applyAttributeToTargetProduct(targetProjection.getId(), productKey, rule, rewrittenValue);
                })
                .exceptionallyCompose(ex -> {
                    if (ex instanceof ApiHttpException && ((ApiHttpException) ex).getStatusCode() == 404) {
                        return CompletableFuture.completedFuture(null);
                    }
                    LOGGER.error("Step-2: Failed reconciling productKey '{}' for rule {}.", productKey, rule, ex);
                    return CompletableFuture.completedFuture(null);
                });
    }

    private Attribute findVariantAttr(ProductVariant variant, String name) {
        if (variant == null || variant.getAttributes() == null) return null;
        for (Attribute a : variant.getAttributes()) {
            if (a != null && name.equals(a.getName())) return a;
        }
        return null;
    }

    private Attribute findProductAttr(List<Attribute> attrs, String name) {
        if (attrs == null) return null;
        for (Attribute a : attrs) {
            if (a != null && name.equals(a.getName())) return a;
        }
        return null;
    }

    @Nonnull
    private CompletionStage<Void> applyAttributeToTargetProduct(
            @Nonnull final String targetProductId,
            @Nonnull final String targetProductKey,
            @Nonnull final Rule rule,
            @Nonnull final Object value) {

        // PERF+SAFETY: fetch by ID (we already have it), not by key.
        return target.products().withId(targetProductId).get().execute()
                .thenCompose(getResp -> {
                    Product p = getResp.getBody();
                    if (p == null) return CompletableFuture.completedFuture(null);

                    boolean published = p.getMasterData() != null
                            && p.getMasterData().getCurrent() != null
                            && Boolean.TRUE.equals(p.getMasterData().getPublished());

                    List<ProductUpdateAction> actions = new ArrayList<>(2);

                    if (rule.isVariant) {
                        actions.add(ProductSetAttributeActionBuilder.of()
                                .name(rule.attributeName)
                                .variantId(rule.variantId)
                                .staged(true)
                                .value(value)
                                .build());
                    } else {
                        actions.add(ProductSetProductAttributeActionBuilder.of()
                                .name(rule.attributeName)
                                .staged(true)
                                .value(value)
                                .build());
                    }

                    if (published) {
                        actions.add(ProductPublishActionBuilder.of().build());
                    }

                    ProductUpdate update = ProductUpdateBuilder.of()
                            .version(p.getVersion())
                            .actions(actions)
                            .build();

                    return target.products().withId(p.getId()).post(update).execute().thenApply(r -> null);
                })
                .thenAccept(ignored -> {});
    }

    private Object rewriteReferencesToTargetIds(Object srcValue) {
        JsonNode node = om.valueToTree(srcValue);
        JsonNode rewritten = rewriteNode(node);
        return rewritten;
    }

    private JsonNode rewriteNode(JsonNode node) {
        if (node == null) return null;

        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node.deepCopy();
            JsonNode typeId = obj.get("typeId");
            JsonNode id = obj.get("id");

            if (typeId != null && "product".equals(typeId.asText()) && id != null && !id.isNull()) {
                String sourceIdOrKey = id.asText();
                String targetId = resolveTargetIdFromSourceIdOrKey(sourceIdOrKey);
                if (targetId != null) {
                    obj.put("id", targetId);
                }
            }

            obj.fields().forEachRemaining(e -> obj.set(e.getKey(), rewriteNode(e.getValue())));
            return obj;
        }

        if (node.isArray()) {
            ArrayNode arr = (ArrayNode) node.deepCopy();
            for (int i = 0; i < arr.size(); i++) {
                arr.set(i, rewriteNode(arr.get(i)));
            }
            return arr;
        }

        return node;
    }

    private String resolveTargetIdFromSourceIdOrKey(String sourceIdOrKey) {
        if (targetKeyToId.containsKey(sourceIdOrKey)) {
            return targetKeyToId.get(sourceIdOrKey);
        }

        String sourceKey = sourceIdOrKey;
        if (looksLikeUuid(sourceIdOrKey)) {
            sourceKey = sourceIdToKey.computeIfAbsent(sourceIdOrKey, this::fetchSourceProductKeyById);
            if (sourceKey == null) {
                return null;
            }
        }

        String targetId = fetchTargetProductIdByKey(sourceKey);
        if (targetId != null) {
            targetKeyToId.put(sourceKey, targetId);
        }
        return targetId;
    }

    private String fetchSourceProductKeyById(String sourceProductId) {
        try {
            ProductProjection src = source.productProjections().withId(sourceProductId).get().withStaged(true).executeBlocking().getBody();
            return src == null ? null : src.getKey();
        } catch (ApiHttpException e) {
            if (e.getStatusCode() == 404) return null;
            throw e;
        }
    }

    private String fetchTargetProductIdByKey(String key) {
        try {
            ProductProjection tp = target.productProjections().withKey(key).get().withStaged(true).executeBlocking().getBody();
            return tp == null ? null : tp.getId();
        } catch (ApiHttpException e) {
            if (e.getStatusCode() == 404) return null;
            throw e;
        }
    }

    private static boolean looksLikeUuid(String s) {
        return s != null && s.length() == 36 && s.chars().filter(ch -> ch == '-').count() == 4;
    }

    private static final class Rule {
        final String productTypeKey;
        final String attributeName;
        final boolean isVariant;
        final Long variantId;

        static Rule variantAttr(String productTypeKey, String attributeName, long variantId) {
            return new Rule(productTypeKey, attributeName, true, variantId);
        }

        static Rule productAttr(String productTypeKey, String attributeName) {
            return new Rule(productTypeKey, attributeName, false, null);
        }

        private Rule(String productTypeKey, String attributeName, boolean isVariant, Long variantId) {
            this.productTypeKey = productTypeKey;
            this.attributeName = attributeName;
            this.isVariant = isVariant;
            this.variantId = variantId;
        }

        @Override
        public String toString() {
            return "Rule{productTypeKey='" + productTypeKey + "', attributeName='" + attributeName
                    + "', isVariant=" + isVariant + ", variantId=" + variantId + "}";
        }
    }

    private boolean attributeNeedsFix(@Nonnull final Attribute attr) {
        final JsonNode node = om.valueToTree(attr.getValue());
        if (node == null || node.isNull()) {
            return true;
        }
        if (node.isArray() && node.size() == 0) {
            return true;
        }
        return containsUuidProductReference(node);
    }

    private boolean containsUuidProductReference(@Nonnull final JsonNode node) {
        if (node.isObject()) {
            final JsonNode typeId = node.get("typeId");
            final JsonNode id = node.get("id");
            if (typeId != null && "product".equals(typeId.asText()) && id != null && looksLikeUuid(id.asText())) {
                return true;
            }
            final var it = node.fields();
            while (it.hasNext()) {
                final var e = it.next();
                if (containsUuidProductReference(e.getValue())) return true;
            }
            return false;
        }

        if (node.isArray()) {
            for (JsonNode child : node) {
                if (containsUuidProductReference(child)) return true;
            }
        }

        return false;
    }

    /*@Nonnull
    private CompletionStage<Void> reconcileTargetProductsOfTypeId(
            @Nonnull final String productTypeId, final int offset, @Nonnull final Rule rule) {

        return target.productProjections().get()
                .withStaged(true)
                .withWhere("productType(id = :ptId)")
                .withPredicateVar("ptId", productTypeId)
                .withLimit(PAGE_SIZE)
                .withOffset(offset)
                .execute()
                .thenCompose(resp -> {
                    List<ProductProjection> results = resp.getBody().getResults();
                    if (results == null || results.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    CompletableFuture<Void> pageChain = CompletableFuture.completedFuture(null);
                    for (ProductProjection tp : results) {
                        pageChain = pageChain.thenCompose(
                                ignored -> reconcileOneTargetProduct(tp, rule).toCompletableFuture());
                    }

                    return pageChain.thenCompose(
                            ignored -> reconcileTargetProductsOfTypeId(productTypeId, offset + PAGE_SIZE, rule)
                                    .toCompletableFuture());
                });
    }*/

    @Nonnull
    private CompletionStage<Void> reconcileTargetProductsOfTypeId(
            @Nonnull final String productTypeId,
            @Nullable final String lastId,
            @Nonnull final Rule rule) {

        // Cursor paging (no offset) to avoid offset > 10000 error.
        // Sort by id asc; then fetch next page with where id > :lastId.
        var req =
                target.productProjections()
                        .get()
                        .withStaged(true)
                        .withSort("id asc")
                        .withLimit(PAGE_SIZE);

        if (lastId == null) {
            req =
                    req.withWhere("productType(id = :ptId)")
                            .withPredicateVar("ptId", productTypeId);
        } else {
            req =
                    req.withWhere("productType(id = :ptId) and id > :lastId")
                            .withPredicateVar("ptId", productTypeId)
                            .withPredicateVar("lastId", lastId);
        }

        return req.execute()
                .thenCompose(
                        resp -> {
                            final List<ProductProjection> results = resp.getBody().getResults();
                            if (results == null || results.isEmpty()) {
                                return CompletableFuture.completedFuture(null);
                            }

                            CompletableFuture<Void> pageChain = CompletableFuture.completedFuture(null);
                            for (ProductProjection tp : results) {
                                pageChain =
                                        pageChain.thenCompose(
                                                ignored -> reconcileOneTargetProduct(tp, rule).toCompletableFuture());
                            }

                            // Continue from the last element's id
                            final String nextLastId = results.get(results.size() - 1).getId();
                            return pageChain.thenCompose(
                                    ignored -> reconcileTargetProductsOfTypeId(productTypeId, nextLastId, rule).toCompletableFuture());
                        });
    }


    @Nonnull
    private CompletionStage<Void> loadProductTypeIdsViaGraphQl() {
        final List<String> keys =
                rules.stream().map(r -> r.productTypeKey).distinct().collect(java.util.stream.Collectors.toList());

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final String where =
                keys.stream()
                        .map(k -> "key=\\\"" + k.replace("\"", "\\\\\"") + "\\\"")
                        .collect(java.util.stream.Collectors.joining(" OR "));

        final String query =
                "query { productTypes(where: \"" + where + "\", limit: " + keys.size() + ") { results { id key } } }";

        final com.commercetools.api.models.graph_ql.GraphQLRequest req =
                com.commercetools.api.models.graph_ql.GraphQLRequestBuilder.of().query(query).build();

        return target.graphql().post(req).execute().thenAccept(resp -> {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode root = mapper.valueToTree(resp.getBody());
            final JsonNode results =
                    root.path("data").path("productTypes").path("results");

            if (!results.isArray()) {
                LOGGER.warn("Step-2: GraphQL productTypes results missing/invalid.");
                return;
            }

            for (JsonNode n : results) {
                final String id = n.path("id").asText(null);
                final String key = n.path("key").asText(null);
                if (id != null && key != null) {
                    productTypeKeyToId.put(key, id);
                }
            }
        });
    }
}