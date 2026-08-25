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

import org.dromara.maxkey.notification.core.event.ChangeOperation;
import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.event.SubjectChangeType;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationPublisher;
import org.dromara.maxkey.persistence.event.IdmEntityChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;

/**
 * 监听 IDM 领域事件，转换为主体变更事件并投递给下游通知系统。
 *
 * <p>这是 notification 模块与 persistence 层之间的唯一桥梁：
 * <ul>
 *   <li>方向：{@code persistence 层发事件 → 通知模块消费}（persistence 不依赖 notification）</li>
 *   <li>内容：过滤 + 类型转换（IDM 通用事件 → 主体变更专用事件）</li>
 *   <li>调度：{@code @Async} 异步执行，不阻塞业务事务提交</li>
 * </ul>
 *
 * <p>阶段 1 只处理 GROUP_MEMBER 类型（用户组成员变更 → 用户主体需要刷新）。
 * 其余实体类型（USER / GROUP / ROLE / ORGANIZATION）在阶段 2 按白名单补全。
 *
 * @author Avatar-Knowledge
 */
public class IdmEntityChangeNotificationListener {

    private static final Logger LOG = LoggerFactory.getLogger(IdmEntityChangeNotificationListener.class);

    private final ChangeNotificationPublisher publisher;
    private final TenantIdMapper tenantIdMapper;

    public IdmEntityChangeNotificationListener(ChangeNotificationPublisher publisher,
                                                TenantIdMapper tenantIdMapper) {
        this.publisher = publisher;
        this.tenantIdMapper = tenantIdMapper;
    }

    @EventListener
    @Async("subjectChangeNotifierExecutor")
    public void onEntityChange(IdmEntityChangeEvent event) {
        if (event == null || event.getEntityId() == null) {
            return;
        }
        try {
            SubjectChangeType subjectType = resolveSubjectType(event.getEntityType());
            if (subjectType == null) {
                // 非主体相关事件（如纯配置项变更），直接忽略
                return;
            }
            ChangeOperation operation = resolveOperation(event.getOperation());
            String tenantId = tenantIdMapper.map(event.getTenantId());
            if (tenantId.isEmpty()) {
                LOG.warn("No tenant mapping for instId={}, skip notification", event.getTenantId());
                return;
            }

            // 对于 GROUP_MEMBER 事件，entityId 就是 memberId（用户 id）
            SubjectChangeEvent subjectEvent = SubjectChangeEvent.of(
                    tenantId, subjectType, event.getEntityId(), operation,
                    "IdmEntityChange:" + event.getEntityType());
            publisher.publish(subjectEvent);
        } catch (RuntimeException failure) {
            // 监听失败不能影响上游业务
            LOG.error("failed to process idm entity change event: " + event, failure);
        }
    }

    /**
     * 将 IDM 实体类型映射到权限主体类型。
     * 只映射我们关心的，其余返回 null 表示跳过。
     */
    private static SubjectChangeType resolveSubjectType(String entityType) {
        if (entityType == null) {
            return null;
        }
        return switch (entityType) {
            case "USER", "GROUP_MEMBER" -> SubjectChangeType.USER;
            case "GROUP" -> SubjectChangeType.GROUP;
            case "ROLE_MEMBER" -> SubjectChangeType.USER;
            case "ROLE" -> SubjectChangeType.ROLE;
            default -> null;
        };
    }

    private static ChangeOperation resolveOperation(String operation) {
        if (operation == null) {
            return ChangeOperation.UPDATE;
        }
        return switch (operation.toUpperCase()) {
            case "CREATE" -> ChangeOperation.CREATE;
            case "DELETE" -> ChangeOperation.DELETE;
            default -> ChangeOperation.UPDATE;
        };
    }
}
