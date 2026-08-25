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

package org.dromara.maxkey.notification.idm.outbox;

import org.dromara.maxkey.notification.core.event.ChangeOperation;
import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.event.SubjectChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * Outbox 数据访问：持久化待投递的主体变更事件。
 *
 * <p>Outbox 模式核心：事件与业务事务在同一个数据库事务里写入 outbox 表，
 * 保证"业务成功则事件一定存在"。独立的 relay 线程异步读取并投递，
 * 投递成功后标记为 DELIVERED。进程崩溃也不会丢事件——重启后 relay 继续。
 *
 * @author Avatar-Knowledge
 */
public class OutboxRepository {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRepository.class);

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 插入一条待投递事件。应该在业务事务内调用。
     */
    public void insert(SubjectChangeEvent event, String entityType, String entityId) {
        jdbc.update(
                "INSERT INTO mxk_notification_outbox " +
                "(id, tenant_id, subject_type, subject_id, operation, source, " +
                " entity_type, entity_id, status, retry_count, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)",
                event.getEventId(),
                event.getTenantId(),
                event.getSubjectType().name(),
                event.getSubjectId(),
                event.getOperation().name(),
                event.getSource(),
                entityType,
                entityId,
                Timestamp.from(Instant.now()));
    }

    /**
     * 拉取一批待投递事件（PENDING 且 next_attempt_at <= now），按创建时间升序。
     * 拉取后立刻标记为"锁定"状态（通过在事务里 SELECT ... FOR UPDATE SKIP LOCKED 更好，
     * 但 MySQL 5.7 不支持 SKIP LOCKED，这里用简单方式：查询后每个事件在投递中
     * 用 CAS 方式更新状态，避免并发重复投递）。
     */
    public List<OutboxEntry> pollPending(int batchSize) {
        return jdbc.query(
                "SELECT * FROM mxk_notification_outbox " +
                "WHERE status = 'PENDING' AND next_attempt_at <= ? " +
                "ORDER BY created_at ASC LIMIT ?",
                new Object[]{Timestamp.from(Instant.now()), batchSize},
                OUTBOX_ROW_MAPPER);
    }

    /**
     * 标记事件为已投递。
     *
     * @return true 表示更新成功（此进程赢得了投递权）；
     *         false 表示已被其他 relay 处理（并发情况下）。
     */
    public boolean markDelivered(String eventId) {
        int rows = jdbc.update(
                "UPDATE mxk_notification_outbox SET status = 'DELIVERED', updated_at = ? WHERE id = ? AND status = 'PENDING'",
                Timestamp.from(Instant.now()), eventId);
        return rows > 0;
    }

    /**
     * 记录一次投递失败，更新重试次数和下次重试时间（指数退避）。
     * 超过 maxRetries 则标记为 FAILED（死信）。
     */
    public void markFailed(String eventId, String error, int retryCount, int maxRetries, long backoffBaseMs) {
        String newStatus = retryCount >= maxRetries ? "FAILED" : "PENDING";
        long delayMs = backoffBaseMs * (1L << Math.min(retryCount, 10)); // 指数退避，最多 2^10 * base
        Timestamp nextAttempt = Timestamp.from(Instant.now().plusMillis(delayMs));

        String truncatedError = error != null && error.length() > 1024
                ? error.substring(0, 1020) + "..." : error;

        jdbc.update(
                "UPDATE mxk_notification_outbox " +
                "SET status = ?, retry_count = ?, next_attempt_at = ?, last_error = ?, updated_at = ? " +
                "WHERE id = ?",
                newStatus, retryCount + 1, nextAttempt, truncatedError,
                Timestamp.from(Instant.now()), eventId);

        if ("FAILED".equals(newStatus)) {
            LOG.error("Outbox event moved to dead-letter after {} retries: id={} error={}",
                    retryCount + 1, eventId, truncatedError);
        }
    }

    /**
     * outbox 表是否存在（启动时自检用）。
     */
    public boolean tableExists() {
        try {
            jdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.tables " +
                    "WHERE table_name = 'mxk_notification_outbox'",
                    Integer.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---- RowMapper ----

    private static final RowMapper<OutboxEntry> OUTBOX_ROW_MAPPER = new RowMapper<OutboxEntry>() {
        @Override
        public OutboxEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OutboxEntry(
                    rs.getString("id"),
                    rs.getString("tenant_id"),
                    SubjectChangeType.valueOf(rs.getString("subject_type")),
                    rs.getString("subject_id"),
                    ChangeOperation.valueOf(rs.getString("operation")),
                    rs.getString("source"),
                    rs.getString("entity_type"),
                    rs.getString("entity_id"),
                    rs.getString("status"),
                    rs.getInt("retry_count"),
                    toInstant(rs.getTimestamp("next_attempt_at"))
            );
        }
    };

    private static Instant toInstant(Timestamp ts) {
        if (ts == null) return Instant.now();
        return ts.toInstant();
    }

    /**
     * outbox 行数据。
     */
    public static class OutboxEntry {
        public final String eventId;
        public final String tenantId;
        public final SubjectChangeType subjectType;
        public final String subjectId;
        public final ChangeOperation operation;
        public final String source;
        public final String entityType;
        public final String entityId;
        public final String status;
        public final int retryCount;
        public final Instant nextAttemptAt;

        public OutboxEntry(String eventId, String tenantId, SubjectChangeType subjectType,
                           String subjectId, ChangeOperation operation, String source,
                           String entityType, String entityId, String status,
                           int retryCount, Instant nextAttemptAt) {
            this.eventId = eventId;
            this.tenantId = tenantId;
            this.subjectType = subjectType;
            this.subjectId = subjectId;
            this.operation = operation;
            this.source = source;
            this.entityType = entityType;
            this.entityId = entityId;
            this.status = status;
            this.retryCount = retryCount;
            this.nextAttemptAt = nextAttemptAt;
        }

        public SubjectChangeEvent toEvent() {
            return SubjectChangeEvent.of(tenantId, subjectType, subjectId, operation, source)
                    .withEventId(eventId);
        }
    }
}
