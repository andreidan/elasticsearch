package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Encapsulates resolving and validating the rollover alias and target index.
 */
public final class RolloverAliasAndIndexResolver {

    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$");

    public static void validateAlias(AliasOrIndex aliasOrIndex) {
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias is a concrete index");
        }
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
        if (alias.getWriteIndex() == null) {
            throw new IllegalArgumentException("source alias [" + alias.getAliasName() + "] does not point to a write index");
        }
    }

    public static String resolveRolloverIndexName(IndexMetaData sourceIndexMetaData,
                                                  @Nullable String rolloverIndexName,
                                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        String unresolvedName = getUnresolvedRolloverIndexName(sourceIndexMetaData, rolloverIndexName, indexNameExpressionResolver);
        return indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
    }

    public static String getUnresolvedRolloverIndexName(IndexMetaData sourceIndexMetaData, @Nullable String rolloverIndexName,
                                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        String sourceProvidedName = sourceIndexMetaData.getSettings()
            .get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, sourceIndexMetaData.getIndex().getName());
        return rolloverIndexName != null
            ? rolloverIndexName
            : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
    }

    static String generateRolloverIndexName(String sourceIndexName, IndexNameExpressionResolver indexNameExpressionResolver) {
        String resolvedName = indexNameExpressionResolver.resolveDateMathExpression(sourceIndexName);
        final boolean isDateMath = sourceIndexName.equals(resolvedName) == false;
        if (INDEX_NAME_PATTERN.matcher(resolvedName).matches()) {
            int numberIndex = sourceIndexName.lastIndexOf("-");
            assert numberIndex != -1 : "no separator '-' found";
            int counter = Integer.parseInt(sourceIndexName.substring(numberIndex + 1, isDateMath ? sourceIndexName.length() - 1 :
                sourceIndexName.length()));
            String newName = sourceIndexName.substring(0, numberIndex) + "-" + String.format(Locale.ROOT, "%06d", ++counter)
                + (isDateMath ? ">" : "");
            return newName;
        } else {
            throw new IllegalArgumentException("index name [" + sourceIndexName + "] does not match pattern '^.*-\\d+$'");
        }
    }

    /**
     * If the newly created index matches with an index template whose aliases contains the rollover alias,
     * the rollover alias will point to multiple indices. This causes indexing requests to be rejected.
     * To avoid this, we make sure that there is no duplicated alias in index templates before creating a new index.
     */
    public static void checkNoDuplicatedAliasInIndexTemplate(MetaData metaData, String rolloverIndexName, String rolloverRequestAlias) {
        final List<IndexTemplateMetaData> matchedTemplates = MetaDataIndexTemplateService.findTemplates(metaData, rolloverIndexName);
        for (IndexTemplateMetaData template : matchedTemplates) {
            if (template.aliases().containsKey(rolloverRequestAlias)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                    rolloverRequestAlias, template.aliases().keys(), template.name()));
            }
        }
    }

    static List<AliasAction> prepareRolloverAliasesUpdateRequest(String oldIndex, String newIndex, String alias) {
        return List.of(
            new AliasAction.Add(newIndex, alias, null, null, null, null),
            new AliasAction.Remove(oldIndex, alias));
    }

    static List<AliasAction> prepareRolloverAliasesWriteIndexUpdateRequest(String oldIndex, String newIndex, String alias) {
        return List.of(
            new AliasAction.Add(newIndex, alias, null, null, null, true),
            new AliasAction.Add(oldIndex, alias, null, null, null, false));
    }

    private RolloverAliasAndIndexResolver() {
    }
}
