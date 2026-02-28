package com.commercetools.project.sync.product;

import com.commercetools.api.client.ProjectApiRoot;
import com.commercetools.api.models.product.Attribute;
import com.commercetools.api.models.product.Product;
import com.commercetools.api.models.product.ProductProjection;
import com.commercetools.api.models.product.ProductSetAttributeActionBuilder;
import com.commercetools.api.models.product.ProductSetProductAttributeActionBuilder;
import com.commercetools.api.models.product.ProductUpdate;
import com.commercetools.api.models.product.ProductUpdateAction;
import com.commercetools.api.models.product.ProductUpdateBuilder;
import com.commercetools.api.models.product.ProductVariant;
import com.commercetools.api.models.product.ProductPublishActionBuilder;
import com.commercetools.api.models.product_type.ProductType;
import com.commercetools.api.models.product_type.ProductTypePagedQueryResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vrap.rmf.base.client.ApiHttpException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
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
    private static final int PAGE_SIZE = 200;

    private final ProjectApiRoot source;
    private final ProjectApiRoot target;
    private final ObjectMapper om = new ObjectMapper();

    // Configure the product types + attributes you want to reconcile.
    // productTypeKey values must match the ProductType "key" in commercetools.
    private final List<Rule> rules = List.of(
            // menu_product: variant attribute product_sku
            Rule.variantAttr("menu_product", "product_sku", 1L),

            // menu_sku: product-level attribute sku_configurable_properties
            Rule.productAttr("menu_sku", "sku_configurable_properties"),

            // configurable-properties: product-level attribute configurationOptions
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
        return loadProductTypeIds()
                .thenCompose(ignored -> reconcileAllRules())
                .thenAccept(ignored -> {});
    }

    @Nonnull
    private CompletionStage<Void> loadProductTypeIds() {
        // Fetch ProductType ids by key for all configured rules.
        final Set<String> keys = new HashSet<>();
        for (Rule r : rules) keys.add(r.productTypeKey);

        return target.productTypes().get().withLimit(PAGE_SIZE).withWithTotal(true).execute()
                .thenCompose(resp -> {
                    ProductTypePagedQueryResponse body = resp.getBody();
                    if (body == null) return CompletableFuture.completedFuture(null);

                    long total = body.getTotal() == null ? 0 : body.getTotal();
                    long pages = (total + PAGE_SIZE - 1) / PAGE_SIZE;

                    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
                    for (int p = 0; p < pages; p++) {
                        int offset = p * PAGE_SIZE;
                        chain = chain.thenCompose(ignored -> target.productTypes().get()
                                .withLimit(PAGE_SIZE)
                                .withOffset(offset)
                                .execute()
                                .thenAccept(r -> {
                                    List<ProductType> pts = r.getBody().getResults();
                                    if (pts == null) return;
                                    for (ProductType pt : pts) {
                                        if (pt.getKey() != null && pt.getId() != null && keys.contains(pt.getKey())) {
                                            productTypeKeyToId.put(pt.getKey(), pt.getId());
                                        }
                                    }
                                }).toCompletableFuture());
                    }
                    return chain;
                });
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
        final String ptId = productTypeKeyToId.get(rule.productTypeKey);
        if (ptId == null) {
            LOGGER.warn("Step-2: ProductType key '{}' not found in TARGET. Skipping rule {}.", rule.productTypeKey, rule);
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.info("Step-2: Reconciling rule {}", rule);
        return reconcileTargetProductsOfType(ptId, 0, rule);
    }

    @Nonnull
    private CompletionStage<Void> reconcileTargetProductsOfType(
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
                        pageChain = pageChain.thenCompose(ignored -> reconcileOneTargetProduct(tp, rule).toCompletableFuture());
                    }

                    return pageChain.thenCompose(ignored -> reconcileTargetProductsOfType(productTypeId, offset + PAGE_SIZE, rule).toCompletableFuture());
                });
    }

    @Nonnull
    private CompletionStage<Void> reconcileOneTargetProduct(@Nonnull final ProductProjection targetProjection, @Nonnull final Rule rule) {
        final String productKey = targetProjection.getKey();
        if (productKey == null || productKey.isBlank()) {
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
                        // Source doesn't have it => nothing to enforce.
                        return CompletableFuture.completedFuture(null);
                    }

                    // Resolve references inside srcAttr value from SOURCE -> TARGET
                    Object rewrittenValue = rewriteReferencesToTargetIds(srcAttr.getValue());
                    if (rewrittenValue == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // If target already has a non-empty attribute, skip (safe-by-default).
                    /*boolean alreadySet = rule.isVariant
                            ? (findVariantAttr(targetProjection.getMasterVariant(), rule.attributeName) != null)
                            : (findProductAttr(targetProjection.getAttributes(), rule.attributeName) != null);

                    if (alreadySet) {
                        return CompletableFuture.completedFuture(null);
                    }*/

                    // target attribute
                    final Attribute targetAttr = rule.isVariant
                            ? findVariantAttr(targetProjection.getMasterVariant(), rule.attributeName)
                            : findProductAttr(targetProjection.getAttributes(), rule.attributeName);

                    // Only skip if target is already "resolved"
                    if (targetAttr != null && !attributeNeedsFix(targetAttr)) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // Otherwise apply (fill missing OR fix unresolved OR fix blank)
                    return applyAttributeToTargetProduct(targetProjection.getId(), productKey, rule, rewrittenValue);
                })
                .exceptionallyCompose(ex -> {
                    // If source product doesn't exist, ignore.
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

        return target.products().withKey(targetProductKey).get().execute()
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

    /**
     * Rewrites any embedded product references:
     * - if reference.id looks like a SOURCE UUID => fetch SOURCE product by id and get its key
     * - then fetch TARGET product by key and use its id
     */
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
        // If already cached as target key -> id
        if (targetKeyToId.containsKey(sourceIdOrKey)) {
            return targetKeyToId.get(sourceIdOrKey);
        }

        // First, interpret as SOURCE product id -> key (if it is a UUID)
        String sourceKey = sourceIdOrKey;
        if (looksLikeUuid(sourceIdOrKey)) {
            sourceKey = sourceIdToKey.computeIfAbsent(sourceIdOrKey, this::fetchSourceProductKeyById);
            if (sourceKey == null) {
                return null;
            }
        }

        // Then resolve TARGET product id by key
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

        // empty array/set => needs fix
        if (node.isArray() && node.size() == 0) {
            return true;
        }

        // If it contains any product reference whose id is a UUID (likely source id) => needs fix
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
}