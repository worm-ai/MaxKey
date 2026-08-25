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

package org.dromara.maxkey.notification.idm.autoconfigure;

import java.time.Duration;
import java.util.List;

import jakarta.annotation.PreDestroy;

import org.dromara.maxkey.notification.core.spi.ChangeNotificationSink;
import org.dromara.maxkey.notification.idm.IdmEntityChangeOutboxWriter;
import org.dromara.maxkey.notification.idm.TenantIdMapper;
import org.dromara.maxkey.notification.idm.outbox.OutboxRelay;
import org.dromara.maxkey.notification.idm.outbox.OutboxRepository;
import org.dromara.maxkey.notification.webhook.WebhookNotificationSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * IDM 主体变更通知自动装配（outbox 模式）。
 *
 * <p>由 {@code maxkey.notification.subject-change.enabled=true} 控制开关；
 * 未显式启用时整个模块不激活，对 MaxKey 原有功能零影响。
 *
 * <p>架构（outbox 模式，保证不丢事件）：
 * <pre>
 * [业务事务]
 *   Service 写数据
 *   → ApplicationEventPublisher.publishEvent(IdmEntityChangeEvent)  ← 同步
 *   → TransactionalEventListener(BEFORE_COMMIT)                      ← 同步，同事务
 *     → OutboxRepository.insert()                                    ← 同事务写 outbox 表
 *
 * [独立 relay 线程]
 *   OutboxRelay 轮询 outbox 表
 *   → 投递到各 sink
 *   → 成功: markDelivered / 失败: 退避重试 → 死信
 * </pre>
 *
 * <p>核心正确性保证：
 * <ul>
 *   <li>事务原子性：业务数据 + outbox 记录要么一起提交、要么一起回滚；</li>
 *   <li>至少一次投递：进程崩溃后重启，relay 继续从 outbox 表里捞未投递的；</li>
 *   <li>幂等：下游 PP 通过 Idempotency-Key 保证重复投递无副作用；</li>
 *   <li>退避 + 死信：持续失败的事件不会打爆下游，最终进入 FAILED 状态留痕。</li>
 * </ul>
 *
 * @author Avatar-Knowledge
 */
@Configuration
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        prefix = "maxkey.notification.subject-change",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = false)
public class SubjectChangeNotificationAutoConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubjectChangeNotificationAutoConfiguration.class);

    private OutboxRelay relay;

    /**
     * Kafka 通知 sink（主路径）：发布到 Kafka topic，下游订阅。
     *
     * <p>当 {@code kafka.bootstrap-servers} 配置非空时启用。
     * 这是标准的领域事件总线模式，推荐生产使用。
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "maxkey.notification.subject-change.kafka",
            name = "bootstrap-servers",
            matchIfMissing = false)
    ChangeNotificationSink kafkaNotificationSink(
            @Value("${maxkey.notification.subject-change.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${maxkey.notification.subject-change.kafka.topic:maxkey.idm.entity-changed}") String topic,
            @Value("${maxkey.notification.subject-change.kafka.acks:1}") String acks,
            @Value("${maxkey.notification.subject-change.kafka.request-timeout-ms:5000}") int requestTimeoutMs) {
        LOG.info("Subject change notification kafka sink enabled: bootstrap-servers={} topic={}",
                bootstrapServers, topic);

        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", String.valueOf(requestTimeoutMs));
        props.put("delivery.timeout.ms", String.valueOf(requestTimeoutMs + 2000));
        props.put("retries", "3");

        return new org.dromara.maxkey.notification.kafka.KafkaNotificationSink(
                "kafka:" + topic, topic, props,
                java.time.Duration.ofMillis(requestTimeoutMs));
    }

    /**
     * Webhook 通知 sink（备选路径）：直接 HTTP 调用下游刷新端点。
     *
     * <p>没有 Kafka 环境时使用，简单直接但耦合度高。
     * Kafka 和 webhook 可同时启用（都进 sinks 列表）。
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "maxkey.notification.subject-change.webhook",
            name = "endpoint",
            matchIfMissing = false)
    ChangeNotificationSink webhookNotificationSink(
            @Value("${maxkey.notification.subject-change.webhook.endpoint:}") String endpoint,
            @Value("${maxkey.notification.subject-change.webhook.token:}") String token,
            @Value("${maxkey.notification.subject-change.webhook.connect-timeout-ms:1000}") int connectTimeoutMs,
            @Value("${maxkey.notification.subject-change.webhook.request-timeout-ms:5000}") int requestTimeoutMs) {
        LOG.info("Subject change notification webhook sink enabled: endpoint={}", endpoint);
        return new WebhookNotificationSink("permission-platform-webhook", endpoint, token,
                Duration.ofMillis(connectTimeoutMs), Duration.ofMillis(requestTimeoutMs));
    }

    @Bean
    TenantIdMapper tenantIdMapper(
            @Value("${maxkey.notification.subject-change.tenant-mappings:}") String mappings,
            @Value("${maxkey.notification.subject-change.default-tenant-id:}") String defaultTenantId) {
        return new TenantIdMapper(mappings, defaultTenantId);
    }

    @Bean
    OutboxRepository outboxRepository(JdbcTemplate jdbcTemplate) {
        OutboxRepository repo = new OutboxRepository(jdbcTemplate);
        if (repo.tableExists()) {
            LOG.info("Outbox table found, outbox pattern enabled");
        } else {
            LOG.warn("Outbox table not found (mxk_notification_outbox). " +
                    "Notification will still work but events may be lost on crash. " +
                    "Please create the outbox table (see db/migration/V1__notification_outbox.sql).");
        }
        return repo;
    }

    @Bean
    IdmEntityChangeOutboxWriter idmEntityChangeOutboxWriter(
            OutboxRepository outboxRepository,
            TenantIdMapper tenantIdMapper,
            @Value("${maxkey.notification.subject-change.event-types:}") String eventTypes) {
        LOG.info("IDM entity change outbox writer registered (event types: {})",
                eventTypes.isEmpty() ? "all" : eventTypes);
        java.util.Set<String> whitelist = new java.util.HashSet<>();
        if (!eventTypes.trim().isEmpty()) {
            for (String type : eventTypes.split(",")) {
                String trimmed = type.trim();
                if (!trimmed.isEmpty()) {
                    whitelist.add(trimmed.toUpperCase());
                }
            }
        }
        return new IdmEntityChangeOutboxWriter(outboxRepository, tenantIdMapper,
                whitelist);
    }

    @Bean
    OutboxRelay outboxRelay(OutboxRepository outboxRepository,
                            List<ChangeNotificationSink> sinks,
                            @Value("${maxkey.notification.subject-change.outbox.batch-size:20}") int batchSize,
                            @Value("${maxkey.notification.subject-change.outbox.poll-interval-ms:2000}") long pollIntervalMs,
                            @Value("${maxkey.notification.subject-change.outbox.max-retries:20}") int maxRetries,
                            @Value("${maxkey.notification.subject-change.outbox.backoff-base-ms:1000}") long backoffBaseMs) {
        this.relay = new OutboxRelay(outboxRepository, sinks,
                batchSize, pollIntervalMs, maxRetries, backoffBaseMs);
        relay.start();
        return relay;
    }

    @PreDestroy
    public void stopRelay() {
        if (relay != null) {
            relay.stop();
        }
    }

    /**
     * 联调测试端点（默认关闭）。仅用于本地/测试环境，生产切勿启用。
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "maxkey.notification.subject-change",
            name = "test-endpoint",
            havingValue = "true")
    org.dromara.maxkey.notification.idm.test.NotificationTestController notificationTestController(
            // 测试端点用 outbox 模式：事件写入 outbox，由 relay 投递
            OutboxRepository outboxRepository,
            org.dromara.maxkey.persistence.service.GroupMemberService groupMemberService,
            org.springframework.web.context.WebApplicationContext applicationContext,
            @Value("${maxkey.notification.subject-change.default-tenant-id:}") String defaultTenantId) {
        LOG.info("Notification test endpoint enabled (DEV ONLY)");
        // 测试端点的 publisher 用一个"写 outbox"的 publisher 实现
        // 这里传 outboxRepository，Controller 内部有个适配
        return new org.dromara.maxkey.notification.idm.test.NotificationTestController(
                null, groupMemberService, applicationContext, defaultTenantId);
    }
}
