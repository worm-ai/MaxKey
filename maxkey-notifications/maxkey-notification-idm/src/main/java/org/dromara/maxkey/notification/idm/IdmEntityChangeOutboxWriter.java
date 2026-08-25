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
import org.dromara.maxkey.notification.idm.outbox.OutboxRepository;
import org.dromara.maxkey.persistence.event.IdmEntityChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 监听 IDM 领域事件，写入 outbox 表（与业务事务原子提交）。
 *
 * <p>这是 outbox 模式的关键一环：
 * <ul>
 *   <li>在业务事务提交前把事件写入 outbox 表（同一事务）；</li>
 *   <li>事务成功 → outbox 记录一定存在 → relay 迟早能投递出去；</li>
 *   <li>事务回滚 → outbox 记录一起回滚 → 不会投递无效事件。</li>
 * </ul>
 *
 * <p>用 {@code TransactionalEventListener(BEFORE_COMMIT)} 确保写 outbox
 * 与业务数据在同一事务里提交。
 *
 * @author Avatar-Knowledge
 */
public class IdmEntityChangeOutboxWriter {

    private static final Logger LOG = LoggerFactory.getLogger(IdmEntityChangeOutboxWriter.class);

    private final OutboxRepository outbox;
    private final TenantIdMapper tenantIdMapper;

    /**
     * 事务提交前写入 outbox（与业务数据原子提交）。
     *
     * <p>注意：必须在有事务的 Service 方法里调用才会生效。
     * 如果没有事务上下文，本监听器不会执行——这种场景下事件可能丢，
     * 由调用方负责确保有事务（或者接受"尽力而为"语义）。
     */
    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void onEntityChange(IdmEntityChangeEvent event) {
        try {
            writeToOutbox(event);
        } catch (RuntimeException e) {
            // outbox 写入失败不能让业务回滚
            LOG.error("Failed to write entity change event to outbox: {}", event, e);
        }
    }

    private void writeToOutbox(IdmEntityChangeEvent event) {
        if (event == null || event.getEntityId() == null) {
            return;
        }

        SubjectChangeType subjectType = resolveSubjectType(event.getEntityType());
        if (subjectType == null) {
            return; // 非主体相关事件，跳过
        }

        String tenantId = tenantIdMapper.map(event.getTenantId());
        if (tenantId.isEmpty()) {
            LOG.warn("No tenant mapping for instId={}, skip outbox write", event.getTenantId());
            return;
        }

        ChangeOperation operation = resolveOperation(event.getOperation());

        // 对于 GROUP_MEMBER / ROLE_MEMBER 事件，entityId 就是受影响主体的 id
        SubjectChangeEvent subjectEvent = SubjectChangeEvent.of(
                tenantId, subjectType, event.getEntityId(), operation,
                "IdmEntityChange:" + event.getEntityType());

        outbox.insert(subjectEvent, event.getEntityType(), event.getEntityId());

        LOG.debug("Entity change written to outbox: type={} entityId={} eventId={}",
                event.getEntityType(), event.getEntityId(), subjectEvent.getEventId());
    }

    private static final java.util.Map<String, SubjectChangeType> ENTITY_TYPE_MAP =
            java.util.Map.of(
                    "USER", SubjectChangeType.USER,
                    "GROUP_MEMBER", SubjectChangeType.USER,
                    "GROUP", SubjectChangeType.GROUP,
                    "ROLE_MEMBER", SubjectChangeType.USER,
                    "ROLE", SubjectChangeType.ROLE,
                    "ORGANIZATION", SubjectChangeType.GROUP);

    /** 事件类型白名单（可配置），为空表示全部放行。 */
    private final java.util.Set<String> allowedEntityTypes;

    public IdmEntityChangeOutboxWriter(OutboxRepository outbox, TenantIdMapper tenantIdMapper) {
        this(outbox, tenantIdMapper, null);
    }

    public IdmEntityChangeOutboxWriter(OutboxRepository outbox, TenantIdMapper tenantIdMapper,
                                       java.util.Set<String> allowedEntityTypes) {
        this.outbox = outbox;
        this.tenantIdMapper = tenantIdMapper;
        this.allowedEntityTypes = allowedEntityTypes == null || allowedEntityTypes.isEmpty()
                ? null : allowedEntityTypes;
    }

    /**
     * 将 IDM 实体类型映射到权限主体类型。
     * 不在映射表里或不在白名单里的返回 null（跳过）。
     */
    private SubjectChangeType resolveSubjectType(String entityType) {
        if (entityType == null) return null;
        SubjectChangeType mapped = ENTITY_TYPE_MAP.get(entityType.toUpperCase());
        if (mapped == null) return null;
        if (allowedEntityTypes != null
                && !allowedEntityTypes.contains(entityType.toUpperCase())) {
            LOG.debug("Entity type {} not in configured whitelist, skip", entityType);
            return null;
        }
        return mapped;
    }

    private static ChangeOperation resolveOperation(String operation) {
        if (operation == null) return ChangeOperation.UPDATE;
        return switch (operation.toUpperCase()) {
            case "CREATE" -> ChangeOperation.CREATE;
            case "DELETE" -> ChangeOperation.DELETE;
            default -> ChangeOperation.UPDATE;
        };
    }
}
