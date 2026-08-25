/*
 * Copyright [2026] [MaxKey of copyright http://www.maxkey.top]
 *
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

package org.dromara.maxkey.notification.idm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * MaxKey instId → 下游系统 tenantId 映射。
 *
 * <p>MaxKey 内部多租户用 {@code instId}（数字字符串如 "1"），
 * 下游权限平台（PP）用租户 code（如 {@code "erp-tenant"}）。
 * 通知出站前需做一次租户 id 转换。
 *
 * <p>阶段 1 用静态配置（YAML 里配好映射表）。
 * 阶段 2 可扩展为动态从数据库读，或放到通用租户映射服务里。
 *
 * @author Avatar-Knowledge
 */
public class TenantIdMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TenantIdMapper.class);

    private final Map<String, String> instToTenant;
    private final String defaultTenantId;

    /**
     * @param mappings       形如 {@code "inst1=tenant1,inst2=tenant2"} 的映射字符串
     * @param defaultTenantId 找不到映射时的兜底租户 id（可为空）
     */
    public TenantIdMapper(String mappings, String defaultTenantId) {
        this.instToTenant = parseMappings(mappings);
        this.defaultTenantId = defaultTenantId != null ? defaultTenantId : "";
        LOG.info("Tenant id mapper initialized: mappings={}, default={}",
                instToTenant.size(), defaultTenantId);
    }

    /**
     * 将 MaxKey instId 转换为下游 tenantId。
     *
     * @param instId MaxKey 的 instId（可能为 null）
     * @return 映射后的 tenantId；找不到映射返回 defaultTenantId
     */
    public String map(String instId) {
        if (instId == null || instId.trim().isEmpty()) {
            return defaultTenantId;
        }
        String mapped = instToTenant.get(instId.trim());
        if (mapped != null) {
            return mapped;
        }
        // 再试 trim 前的
        mapped = instToTenant.get(instId);
        return mapped != null ? mapped : defaultTenantId;
    }

    private static Map<String, String> parseMappings(String mappings) {
        if (mappings == null || mappings.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (String pair : mappings.split(",")) {
            pair = pair.trim();
            if (pair.isEmpty() || !pair.contains("=")) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = pair.substring(0, eq).trim();
            String value = pair.substring(eq + 1).trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                result.put(key, value);
            }
        }
        return result;
    }
}
