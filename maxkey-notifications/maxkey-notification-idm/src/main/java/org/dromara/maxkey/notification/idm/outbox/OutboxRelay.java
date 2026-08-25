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

import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationPublisher;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationSink;
import org.dromara.maxkey.notification.core.spi.SinkDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Outbox relay：轮询 outbox 表，把 PENDING 事件投递给下游 sink。
 *
 * <p>工作流程：
 * <ol>
 *   <li>按批拉取 PENDING 且 next_attempt_at <= now 的事件；</li>
 *   <li>逐条投递，成功则 markDelivered（CAS 方式防止并发重复）；</li>
 *   <li>失败则 markFailed（指数退避 + 计数，超过上限进死信）；</li>
 *   <li>没拉到数据则 sleep 一会儿再轮询。</li>
 * </ol>
 *
 * <p>单 relay 线程足够（事件量不大的场景）；如需水平扩展，
 * 可以加多个 relay 实例 + 用数据库行锁/分布式锁避免重复投递。
 * 由于下游 PP 支持幂等（Idempotency-Key = eventId），
 * 即使偶尔重复投递也不会造成脏数据，所以这里故意不做复杂的锁机制。
 *
 * @author Avatar-Knowledge
 */
public class OutboxRelay implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final List<ChangeNotificationSink> sinks;
    private final int batchSize;
    private final long pollIntervalMs;
    private final int maxRetries;
    private final long backoffBaseMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread relayThread;

    public OutboxRelay(OutboxRepository outbox, List<ChangeNotificationSink> sinks,
                       int batchSize, long pollIntervalMs, int maxRetries, long backoffBaseMs) {
        this.outbox = outbox;
        this.sinks = sinks;
        this.batchSize = batchSize;
        this.pollIntervalMs = pollIntervalMs;
        this.maxRetries = maxRetries;
        this.backoffBaseMs = backoffBaseMs;
    }

    /** 启动 relay 线程。 */
    public void start() {
        if (running.compareAndSet(false, true)) {
            relayThread = new Thread(this, "notification-outbox-relay");
            relayThread.setDaemon(true);
            relayThread.start();
            LOG.info("Outbox relay started: batchSize={} pollIntervalMs={} maxRetries={} sinks={}",
                    batchSize, pollIntervalMs, maxRetries, sinks.size());
        }
    }

    /** 优雅停止。 */
    public void stop() {
        running.set(false);
        if (relayThread != null) {
            relayThread.interrupt();
            try {
                relayThread.join(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Outbox relay stopped");
    }

    @Override
    public void run() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<OutboxRepository.OutboxEntry> batch = outbox.pollPending(batchSize);
                if (batch.isEmpty()) {
                    Thread.sleep(pollIntervalMs);
                    continue;
                }

                for (OutboxRepository.OutboxEntry entry : batch) {
                    if (!running.get()) break;
                    deliverEntry(entry);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Outbox relay error, will retry after {}ms: {}", pollIntervalMs, e.getMessage());
                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void deliverEntry(OutboxRepository.OutboxEntry entry) {
        SubjectChangeEvent event = entry.toEvent();
        boolean allDelivered = true;
        StringBuilder errors = new StringBuilder();

        for (ChangeNotificationSink sink : sinks) {
            try {
                sink.deliver(event);
            } catch (SinkDeliveryException e) {
                allDelivered = false;
                errors.append(sink.name()).append(": ").append(e.getMessage()).append("; ");
                LOG.warn("Outbox delivery failed for sink={} event={} retry={}: {}",
                        sink.name(), event.getEventId(), entry.retryCount, e.getMessage());
            } catch (Exception e) {
                allDelivered = false;
                errors.append(sink.name()).append(": ").append(e.toString()).append("; ");
                LOG.warn("Outbox delivery unexpected error sink={} event={}: {}",
                        sink.name(), event.getEventId(), e.getMessage());
            }
        }

        if (allDelivered) {
            // CAS：只有当状态还是 PENDING 时才标记为 DELIVERED，
            // 防止多实例并发下重复标记
            boolean claimed = outbox.markDelivered(event.getEventId());
            if (claimed) {
                LOG.debug("Outbox event delivered: id={}", event.getEventId());
            }
            // 没拿到（已被其他实例投递）也不算错，幂等的
        } else {
            outbox.markFailed(event.getEventId(), errors.toString(),
                    entry.retryCount, maxRetries, backoffBaseMs);
        }
    }
}
