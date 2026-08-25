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

package org.dromara.maxkey.notification.core.spi;

import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;

/**
 * 变更发布入口 SPI：业务侧只依赖这一个接口。
 *
 * <p>实现须保证 <b>publish 绝不抛出异常、绝不阻塞调用方</b> ——
 * 业务写事务不能因为通知链路故障而失败。投递失败由实现内部记录/重试/落 outbox。
 *
 * @author Avatar-Knowledge
 */
public interface ChangeNotificationPublisher {

    /**
     * 发布一个主体变更事件。
     *
     * <p>本方法必须是「尽力而为且永不抛异常」的：任何内部故障都只记录日志，
     * 不向调用方传播。调用方通常处于业务写事务中。
     *
     * @param event 变更事件；为 null 时静默忽略
     */
    void publish(SubjectChangeEvent event);
}
