/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package org.apache.kafka.server.quota;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Quota;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class StaticQuotaConfig extends AbstractConfig {
    private static final String PRODUCE_QUOTA_PROP = "client.quota.callback.static.produce";
    private static final String FETCH_QUOTA_PROP = "client.quota.callback.static.fetch";
    private static final String REQUEST_QUOTA_PROP = "client.quota.callback.static.request";
    private static final String STORAGE_QUOTA_SOFT_PROP = "client.quota.callback.static.storage.soft";
    private static final String STORAGE_QUOTA_HARD_PROP = "client.quota.callback.static.storage.hard";
    private static final String STORAGE_CHECK_INTERVAL_PROP = "client.quota.callback.static.storage.check-interval";
    private static final String DISABLE_QUOTA_ANONYMOUS_PROP = "client.quota.callback.static.disable-quota-anonymous";
    private static final String LOG_DIRS_PROP = "log.dirs";

    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)")
                        .define(STORAGE_QUOTA_SOFT_PROP, LONG, Long.MAX_VALUE, HIGH, "Hard limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_HARD_PROP, LONG, Long.MAX_VALUE, HIGH, "Soft limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_CHECK_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between storage check runs (default of 0 means disabled")
                        .define(DISABLE_QUOTA_ANONYMOUS_PROP, BOOLEAN, false, MEDIUM, "If set true, anonymous users will be be subjected to the quota")
                        .define(LOG_DIRS_PROP, STRING, "/tmp/kafka-logs", HIGH, "Broker log directory"),
                props,
                doLog);
    }

    Map<ClientQuotaType, Quota> getQuotaMap() {
        Map<ClientQuotaType, Quota> m = new HashMap<>();
        Double produceBound = getDouble(PRODUCE_QUOTA_PROP);
        Double fetchBound = getDouble(FETCH_QUOTA_PROP);
        Double requestBound = getDouble(REQUEST_QUOTA_PROP);

        m.put(ClientQuotaType.PRODUCE, Quota.upperBound(produceBound));
        m.put(ClientQuotaType.FETCH, Quota.upperBound(fetchBound));
        m.put(ClientQuotaType.REQUEST, Quota.upperBound(requestBound));

        return m;
    }

    long getHardStorageQuota() {
        return getLong(STORAGE_QUOTA_HARD_PROP);
    }

    long getSoftStorageQuota() {
        return getLong(STORAGE_QUOTA_SOFT_PROP);
    }

    int getStorageCheckInterval() {
        return getInt(STORAGE_CHECK_INTERVAL_PROP);
    }

    String getLogDirs() {
        return getString(LOG_DIRS_PROP);
    }

    boolean isDisableQuotaAnonymous() {
        return getBoolean(DISABLE_QUOTA_ANONYMOUS_PROP) != null && getBoolean(DISABLE_QUOTA_ANONYMOUS_PROP);
    }
}

