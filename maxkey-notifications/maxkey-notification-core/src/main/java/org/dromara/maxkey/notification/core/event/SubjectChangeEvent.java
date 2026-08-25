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

package org.dromara.maxkey.notification.core.event;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * 主体变更领域事件。
 *
 * <p>这是通知内核对外的核心契约：描述「哪个租户下、哪个主体、发生了什么操作」，
 * 不含任何投递通道细节（HTTP/MQ/webhook 均由 sink 决定），也不含任何 MaxKey 类型引用，
 * 以便整体上提到 framework 层复用。
 *
 * <p>不可变对象，可安全跨线程传递。
 *
 * @author Avatar-Knowledge
 */
public final class SubjectChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 事件唯一 id，用作下游幂等键。 */
    private final String eventId;

    /** 租户 id。MaxKey 侧对应 instId。 */
    private final String tenantId;

    /** 主体类型。 */
    private final SubjectChangeType subjectType;

    /** 主体 id。对 USER 类型即用户 id。 */
    private final String subjectId;

    /** 变更操作。 */
    private final ChangeOperation operation;

    /** 事件产生时间（epoch millis）。 */
    private final long occurredAtEpochMillis;

    /**
     * 变更来源标识，便于排查（如 "GroupMemberServiceImpl.insert"）。
     * 仅用于日志与审计，下游不应依赖其取值。
     */
    private final String source;

    private SubjectChangeEvent(String eventId, String tenantId, SubjectChangeType subjectType,
                               String subjectId, ChangeOperation operation,
                               long occurredAtEpochMillis, String source) {
        this.eventId = eventId;
        this.tenantId = tenantId;
        this.subjectType = subjectType;
        this.subjectId = subjectId;
        this.operation = operation;
        this.occurredAtEpochMillis = occurredAtEpochMillis;
        this.source = source;
    }

    /**
     * 创建一个主体变更事件。
     *
     * @param tenantId    租户 id，必填
     * @param subjectType 主体类型，必填
     * @param subjectId   主体 id，必填
     * @param operation   变更操作，必填
     * @param source      来源标识，可为 null
     * @return 不可变事件实例
     * @throws IllegalArgumentException 必填项缺失时抛出
     */
    public static SubjectChangeEvent of(String tenantId, SubjectChangeType subjectType,
                                        String subjectId, ChangeOperation operation,
                                        String source) {
        requireText(tenantId, "tenantId");
        requireText(subjectId, "subjectId");
        if (subjectType == null) {
            throw new IllegalArgumentException("subjectType is required");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation is required");
        }
        return new SubjectChangeEvent(UUID.randomUUID().toString(), tenantId.trim(), subjectType,
                subjectId.trim(), operation, System.currentTimeMillis(), source);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    public String getEventId() {
        return eventId;
    }

    /**
     * 返回一个指定 eventId 的新事件（用于从 outbox 恢复时复用数据库里的 id）。
     * 这是 outbox relay 专用 API，保证下游幂等键一致。
     */
    public SubjectChangeEvent withEventId(String eventId) {
        return new SubjectChangeEvent(eventId, this.tenantId, this.subjectType,
                this.subjectId, this.operation, this.occurredAtEpochMillis, this.source);
    }

    public String getTenantId() {
        return tenantId;
    }

    public SubjectChangeType getSubjectType() {
        return subjectType;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public ChangeOperation getOperation() {
        return operation;
    }

    public long getOccurredAtEpochMillis() {
        return occurredAtEpochMillis;
    }

    public String getSource() {
        return source;
    }

    /**
     * 去重键：同一租户下同一主体的多次变更在聚合窗口内应合并为一次刷新。
     *
     * <p>刻意不含 operation 与 eventId —— 下游刷新是「重新拉取当前事实」的幂等动作，
     * 无论期间发生过几次增删改，最终只需刷一次。
     *
     * @return 用于聚合去重的键
     */
    public String dedupKey() {
        return tenantId + '\u001f' + subjectType.name() + '\u001f' + subjectId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SubjectChangeEvent)) {
            return false;
        }
        SubjectChangeEvent that = (SubjectChangeEvent) other;
        return eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }

    @Override
    public String toString() {
        return "SubjectChangeEvent{" + "eventId='" + eventId + '\''
                + ", tenantId='" + tenantId + '\''
                + ", subjectType=" + subjectType
                + ", subjectId='" + subjectId + '\''
                + ", operation=" + operation
                + ", source='" + source + '\'' + '}';
    }
}
