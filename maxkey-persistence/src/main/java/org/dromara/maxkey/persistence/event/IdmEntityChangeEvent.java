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

package org.dromara.maxkey.persistence.event;

import org.springframework.context.ApplicationEvent;

/**
 * IDM 实体变更领域事件。
 *
 * <p>由 persistence 层在实体写操作成功后通过 {@code ApplicationEventPublisher} 发布，
 * 由感兴趣的模块通过 {@code @EventListener} 订阅（如通知、审计日志、缓存失效等）。
 *
 * <p>这是 IDM 写入口的标准化事件出口：任何 IDM 实体（用户 / 组 / 角色 / 成员 / 组织）
 * 的新增 / 修改 / 删除，都应该发布此事件。事件本身不含业务逻辑，只携带「谁变了、
 * 什么类型、什么操作」的最小信息。
 *
 * <p>注意：事件发布在业务事务内同步执行，消费方绝不可做耗时操作，
 * 需要异步投递的请自行转发到线程池或消息队列。
 *
 * @author Avatar-Knowledge
 */
public class IdmEntityChangeEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1L;

    /** 实体类型。 */
    private final String entityType;

    /** 实体 id。 */
    private final String entityId;

    /** 操作类型：CREATE / UPDATE / DELETE。 */
    private final String operation;

    /** 租户 id（instId）。可为空，消费方需自行兜底。 */
    private final String tenantId;

    /**
     * 创建一个 IDM 实体变更事件。
     *
     * @param source     事件源（通常是 Service bean）
     * @param entityType 实体类型，如 "USER" / "GROUP" / "GROUP_MEMBER"
     * @param entityId   实体主键
     * @param operation  操作类型：CREATE / UPDATE / DELETE
     * @param tenantId   租户 id（instId），可为 null
     */
    public IdmEntityChangeEvent(Object source, String entityType, String entityId,
                                String operation, String tenantId) {
        super(source);
        this.entityType = entityType;
        this.entityId = entityId;
        this.operation = operation;
        this.tenantId = tenantId;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getOperation() {
        return operation;
    }

    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String toString() {
        return "IdmEntityChangeEvent{" + "entityType='" + entityType + '\''
                + ", entityId='" + entityId + '\''
                + ", operation='" + operation + '\''
                + ", tenantId='" + tenantId + '\'' + '}';
    }
}
