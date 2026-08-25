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

package org.dromara.maxkey.notification.core.dispatch;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationPublisher;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationSink;
import org.dromara.maxkey.notification.core.spi.SinkDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步分发器：把事件投递从业务线程摘出去。
 *
 * <p>设计要点：
 * <ul>
 *   <li><b>永不阻塞业务线程</b>：{@link #publish} 只做入队，队列满时丢弃并告警
 *       （宁可丢通知也不能拖垮 IDM 写操作 —— TTL 兜底仍会让投影最终收敛）；</li>
 *   <li><b>永不抛异常</b>：所有 sink 异常在工作线程内消化；</li>
 *   <li><b>失败留痕</b>：每一次投递失败都打日志，绝不静默吞掉。</li>
 * </ul>
 *
 * <p>阶段 1（最小闭环）刻意不做：outbox 持久化、时间窗去重聚合、指数退避重试。
 * 这些在阶段 2 补齐，届时本类只需替换内部实现，SPI 契约不变。
 *
 * @author Avatar-Knowledge
 */
public final class AsyncNotificationDispatcher implements ChangeNotificationPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncNotificationDispatcher.class);

    private final List<ChangeNotificationSink> sinks;
    private final ExecutorService workers;
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    /**
     * @param sinks         投递目标列表，非空
     * @param workerThreads 工作线程数，建议 1~4
     * @param queueCapacity 待投递队列容量，满则丢弃并告警
     */
    public AsyncNotificationDispatcher(List<ChangeNotificationSink> sinks, int workerThreads,
                                       int queueCapacity) {
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("at least one sink is required");
        }
        if (workerThreads < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("workerThreads and queueCapacity must be positive");
        }
        this.sinks = List.copyOf(sinks);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(workerThreads, workerThreads,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable, "subject-change-notifier");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        this.workers = executor;
    }

    @Override
    public void publish(SubjectChangeEvent event) {
        if (event == null) {
            return;
        }
        published.incrementAndGet();
        try {
            workers.execute(() -> deliverToAll(event));
        } catch (RejectedExecutionException rejected) {
            dropped.incrementAndGet();
            // 队列已满：丢弃通知而不是拖慢业务。投影 TTL 到期后仍会自然失效，
            // 因此这里是「延迟收敛」而非「永久错误」，但必须告警。
            LOG.error("subject change notification dropped, queue is full: event={} droppedTotal={}",
                    event, dropped.get());
        } catch (RuntimeException unexpected) {
            dropped.incrementAndGet();
            LOG.error("subject change notification dropped unexpectedly: event=" + event, unexpected);
        }
    }

    private void deliverToAll(SubjectChangeEvent event) {
        for (ChangeNotificationSink sink : sinks) {
            try {
                sink.deliver(event);
                delivered.incrementAndGet();
                LOG.info("subject change delivered: sink={} tenant={} subjectType={} subjectId={} op={} eventId={}",
                        sink.name(), event.getTenantId(), event.getSubjectType().wireName(),
                        event.getSubjectId(), event.getOperation(), event.getEventId());
            } catch (SinkDeliveryException failure) {
                failed.incrementAndGet();
                LOG.error("subject change delivery failed: sink=" + sink.name()
                        + " transient=" + failure.isTransient() + " event=" + event, failure);
            } catch (RuntimeException unexpected) {
                failed.incrementAndGet();
                LOG.error("subject change delivery crashed: sink=" + sink.name()
                        + " event=" + event, unexpected);
            }
        }
    }

    /**
     * 关闭分发器，等待在途投递完成。
     *
     * @param timeout 最长等待时间
     */
    public void shutdown(long timeout, TimeUnit unit) {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(timeout, unit)) {
                workers.shutdownNow();
                LOG.warn("notification dispatcher did not terminate in time; pending deliveries dropped");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    /**
     * 运行统计，便于健康检查与排查。
     *
     * @return 形如 published/delivered/failed/dropped 的快照
     */
    public String stats() {
        return "published=" + published.get() + " delivered=" + delivered.get()
                + " failed=" + failed.get() + " dropped=" + dropped.get();
    }
}
