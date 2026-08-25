/*
 * Copyright [2026] [MaxKey of copyright http://www.maxkey.top]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.dromara.maxkey.notification.kafka;

import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationSink;
import org.dromara.maxkey.notification.core.spi.SinkDeliveryException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka 通知 sink：把主体变更事件发到 Kafka topic。
 *
 * <p>下游系统（如 permission-platform）通过订阅 topic 感知 IDM 变更，
 * 这是标准的领域事件总线模式：MaxKey 只负责发事件，不关心谁消费。
 *
 * <p>消息格式（JSON）：
 * <pre>
 * {
 *   "eventId": "...",
 *   "tenantId": "erp-tenant",
 *   "subjectType": "USER",
 *   "subjectId": "...",
 *   "operation": "UPDATE",
 *   "source": "IdmEntityChange:GROUP_MEMBER",
 *   "occurredAt": 1724589600000
 * }
 * </pre>
 *
 * <p>Kafka key = {@code tenantId:subjectType:subjectId}，保证同一主体的
 * 变更事件按顺序消费。
 *
 * @author Avatar-Knowledge
 */
public class KafkaNotificationSink implements ChangeNotificationSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationSink.class);

    private final String name;
    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Duration ackTimeout;

    public KafkaNotificationSink(String name, String topic, Properties producerProps,
                                 Duration ackTimeout) {
        this.name = name;
        this.topic = topic;
        this.producer = new KafkaProducer<>(producerProps);
        this.ackTimeout = ackTimeout;
        this.objectMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        LOG.info("Kafka notification sink ready: topic={} name={}", topic, name);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void deliver(SubjectChangeEvent event) throws SinkDeliveryException {
        try {
            String key = buildKey(event);
            String value = objectMapper.writeValueAsString(buildPayload(event));

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add("eventId", event.getEventId().getBytes());
            record.headers().add("subjectType", event.getSubjectType().name().getBytes());

            // 同步等待 ack（outbox relay 已经是异步了，这里同步发确保结果可知）
            producer.send(record).get(ackTimeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);

            LOG.debug("Kafka notification delivered: topic={} key={} eventId={}",
                    topic, key, event.getEventId());

        } catch (TimeoutException | java.util.concurrent.TimeoutException te) {
            throw SinkDeliveryException.transientFailure(name, te);
        } catch (org.apache.kafka.common.errors.InterruptException ie) {
            Thread.currentThread().interrupt();
            throw SinkDeliveryException.transientFailure(name, ie);
        } catch (Exception e) {
            // 不可恢复错误：配置错/topic 不存在等
            throw SinkDeliveryException.definitiveFailure(name, e);
        }
    }

    private String buildKey(SubjectChangeEvent event) {
        return event.getTenantId() + ":" + event.getSubjectType().name() + ":" + event.getSubjectId();
    }

    private Map<String, Object> buildPayload(SubjectChangeEvent event) {
        return Map.of(
                "eventId", event.getEventId(),
                "tenantId", event.getTenantId(),
                "subjectType", event.getSubjectType().name(),
                "subjectId", event.getSubjectId(),
                "operation", event.getOperation().name(),
                "source", event.getSource() != null ? event.getSource() : "",
                "occurredAt", event.getOccurredAtEpochMillis()
        );
    }

    public void close() {
        producer.close(ackTimeout);
        LOG.info("Kafka notification sink closed: name={}", name);
    }
}
