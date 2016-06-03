/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.conf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

/**
 * Class contains all session-based properties for the Accumulo connector.
 * Use SHOW SESSION to view all available properties in the Presto CLI.
 * <br>
 * Can set the property using:
 * <br>
 * <br>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class AccumuloSessionProperties
{
    private static final String INT_OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
    private static final String INT_OPTIMIZE_SPLIT_RANGES_ENABLED = "optimize_split_ranges_enabled";
    private static final String INT_OPTIMIZE_INDEX_ENABLED = "optimize_index_enabled";
    private static final String INT_INDEX_ROWS_PER_SPLIT = "index_rows_per_split";
    private static final String INT_INDEX_THRESHOLD = "index_threshold";
    private static final String INT_INDEX_LOWEST_CARDINALITY_THRESHOLD =
            "index_lowest_cardinality_threshold";
    private static final String INT_INDEX_METRICS_ENABLED = "index_metrics_enabled";
    private static final String INT_SCAN_USERNAME = "scan_username";
    private static final String INT_INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH = "index_short_circuit_cardinality_fetch";
    private static final String INT_INDEX_CARDINALITY_CACHE_POLLING_DURATION =
            "index_cardinality_cache_polling_duration";

    public static final String OPTIMIZE_LOCALITY_ENABLED =
            "accumulo." + INT_OPTIMIZE_LOCALITY_ENABLED;
    public static final String OPTIMIZE_RANGE_SPLITS_ENABLED =
            "accumulo." + INT_OPTIMIZE_SPLIT_RANGES_ENABLED;
    public static final String OPTIMIZE_INDEX_ENABLED = "accumulo." + INT_OPTIMIZE_INDEX_ENABLED;
    public static final String INDEX_ROWS_PER_SPLIT = "accumulo." + INT_INDEX_ROWS_PER_SPLIT;
    public static final String INDEX_THRESHOLD = "accumulo." + INT_INDEX_THRESHOLD;
    public static final String INDEX_LOWEST_CARDINALITY_THRESHOLD =
            "accumulo." + INT_INDEX_LOWEST_CARDINALITY_THRESHOLD;
    public static final String INDEX_METRICS_ENABLED = "accumulo." + INT_INDEX_METRICS_ENABLED;
    public static final String SCAN_USERNAME = "accumulo." + INT_SCAN_USERNAME;
    public static final String INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH = "accumulo." + INT_INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH;
    public static final String INDEX_CARDINALITY_CACHE_POLLING_DURATION = "accumulo." + INT_INDEX_CARDINALITY_CACHE_POLLING_DURATION;

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public AccumuloSessionProperties()
    {
        PropertyMetadata<Boolean> s1 = booleanSessionProperty(INT_OPTIMIZE_LOCALITY_ENABLED,
                "Set to true to enable data locality for non-indexed scans. Default true.", true,
                false);
        PropertyMetadata<Boolean> s2 = booleanSessionProperty(INT_OPTIMIZE_SPLIT_RANGES_ENABLED,
                "Set to true to split non-indexed queries by tablet splits. Should generally be true.",
                true, false);
        PropertyMetadata<String> s3 =
                stringSessionProperty(INT_SCAN_USERNAME,
                        "User to impersonate when scanning the tables. "
                                + "This property trumps the scan_auths table property. "
                                + "Default is the user in the configuration file.", null, false);

        // Properties for secondary index
        PropertyMetadata<Boolean> s4 = booleanSessionProperty(INT_OPTIMIZE_INDEX_ENABLED,
                "Set to true to enable usage of the secondary index on query. Default true.", true,
                false);
        PropertyMetadata<Integer> s5 = integerSessionProperty(INT_INDEX_ROWS_PER_SPLIT,
                "The number of Accumulo row IDs that are packed into a single Presto split. "
                        + "Default 10000",
                10000, false);
        PropertyMetadata<Double> s6 = doubleSessionProperty(INT_INDEX_THRESHOLD,
                "The ratio between number of rows to be scanned based on the index over "
                        + "the total number of rows. If the ratio is below this threshold, "
                        + "the index will be used. Default .2",
                0.2, false);
        PropertyMetadata<Double> s7 = doubleSessionProperty(INT_INDEX_LOWEST_CARDINALITY_THRESHOLD,
                "The threshold where the column with the lowest cardinality will be used instead "
                        + "of computing an intersection of ranges in the secondary index. "
                        + "Secondary index must be enabled. Default .01",
                .01, false);
        PropertyMetadata<Boolean> s8 = booleanSessionProperty(INT_INDEX_METRICS_ENABLED,
                "Set to true to enable usage of the metrics table to optimize usage of the index. "
                        + "Default true", true, false);
        PropertyMetadata<Boolean> s9 = booleanSessionProperty(INT_INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH,
                "Short circuit the retrieval of index metrics once any column is less than the lowest cardinality threshold. "
                        + "Default true", true, false);
        PropertyMetadata<String> s10 =
                new PropertyMetadata<>(INT_INDEX_CARDINALITY_CACHE_POLLING_DURATION,
                        "Sets the cardinality cache polling duration for short circuit retrieval of index metrics. Default 10ms",
                        VarcharType.VARCHAR, String.class,
                        "10ms",
                        false,
                        x -> Duration.valueOf(x.toString()).toString(),
                        object -> object);

        sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
    }

    /**
     * Gets all available session properties.
     *
     * @return The list of session properties
     */
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * Gets a Boolean value indicating whether or not the locality optimization is enabled.
     *
     * @param session The current session
     * @return True if enabled, false otherwise
     */
    public static boolean isOptimizeLocalityEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_LOCALITY_ENABLED, Boolean.class);
    }

    /**
     * Gets a Boolean value indicating whether or not ranges in non-indexed scans will
     * be split on tablets
     *
     * @param session The current session
     * @return True if enabled, false otherwise
     */
    public static boolean isOptimizeSplitRangesEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_SPLIT_RANGES_ENABLED, Boolean.class);
    }

    /**
     * Gets a Boolean value indicating whether or not utilization of the index is enabled
     *
     * @param session The current session
     * @return True if enabled, false otherwise
     */
    public static boolean isOptimizeIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_OPTIMIZE_INDEX_ENABLED, Boolean.class);
    }

    /**
     * Gets the configured threshold for using the index.
     * <br>
     * Queries that would scan a percentage of the tablet greater than this value will
     * execute a full table scan instead.
     *
     * @param session The current session
     * @return The index threshold, 0 - 1
     */
    public static double getIndexThreshold(ConnectorSession session)
    {
        return session.getProperty(INT_INDEX_THRESHOLD, Double.class);
    }

    /**
     * Gets the number of rows IDs, retrieved from the index, to pack into a single Presto split
     *
     * @param session The current session
     * @return The number of rows to put in a single split
     */
    public static int getNumIndexRowsPerSplit(ConnectorSession session)
    {
        return session.getProperty(INT_INDEX_ROWS_PER_SPLIT, Integer.class);
    }

    /**
     * Gets the configured threshold for using the column with the lowest cardinality, instead of
     * computing the index.
     * <br>
     * The connector typically computes an intersection of row IDs across all indexed columns,
     * but if the column with the lowest cardinality is significantly small, we can just use these
     * rows and let Presto filter out the rows that do not match the remaining predicates.
     *
     * @param session The current session
     * @return The index threshold, 0 - 1
     */
    public static double getIndexSmallCardThreshold(ConnectorSession session)
    {
        return session.getProperty(INT_INDEX_LOWEST_CARDINALITY_THRESHOLD, Double.class);
    }

    /**
     * Gets the polling interval for the completion service that fetches cardinalities from Accumulo
     * <br>
     * The LoadingCache is not ordered and, as a result, some cached results (or a result retrieved
     * from Accumulo in a short time) that have higher cardinalities are returned a few milliseconds
     * before a significantly lower result. This parametmer controls the poll duration, adding 'waves
     * of result retrieval from the LoadingCache. The results of any completed tasks are taken,
     * and the smallest cardinality, if below the threshold, is used while the other tasks complete.
     *
     * @param session The current session
     * @return The cardinality cache polling duration
     */
    public static Duration getIndexCardinalityCachePollingDuration(ConnectorSession session)
    {
        return Duration.valueOf(session.getProperty(INT_INDEX_CARDINALITY_CACHE_POLLING_DURATION, String.class));
    }

    /**
     * Gets a Boolean value indicating whether or not utilization of the index metrics is enabled
     *
     * @param session The current session
     * @return True if enabled, false otherwise
     */
    public static boolean isIndexMetricsEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_INDEX_METRICS_ENABLED, Boolean.class);
    }

    /**
     * Gets the user to impersonate during the scan of a table
     *
     * @param session The current session
     * @return The set user, or null if not set
     */
    public static String getScanUsername(ConnectorSession session)
    {
        return session.getProperty(INT_SCAN_USERNAME, String.class);
    }

    /**
     * Gets a Boolean value indicating whether or not index metrics retrieval will be short circuited
     * once any cardinality is below the threshold
     *
     * @param session The current session
     * @return True if enabled, false otherwise
     */
    public static boolean isIndexShortCircuitEnabled(ConnectorSession session)
    {
        return session.getProperty(INT_INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH, Boolean.class);
    }
}
