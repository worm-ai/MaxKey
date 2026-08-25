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
 * 变更投递目标 SPI：一个下游系统一个实现。
 *
 * <p>实现须遵守的契约：
 * <ul>
 *   <li><b>幂等</b>：同一 {@code event.getEventId()} 可能被投递多次（重试、outbox 补投），
 *       实现应将其作为幂等键传给下游；</li>
 *   <li><b>不得抛出受检异常</b>：投递失败以 {@link SinkDeliveryException} 表达，
 *       由分发器决定重试策略；</li>
 *   <li><b>自带超时</b>：实现必须为自身的外部调用设置连接/读取超时，
 *       分发器不负责打断阻塞的 sink。</li>
 * </ul>
 *
 * @author Avatar-Knowledge
 */
public interface ChangeNotificationSink {

    /**
     * sink 名称，用于日志与配置定位。
     *
     * @return 稳定的 sink 名称
     */
    String name();

    /**
     * 投递一个变更事件。
     *
     * @param event 待投递事件，非 null
     * @throws SinkDeliveryException 投递失败
     */
    void deliver(SubjectChangeEvent event) throws SinkDeliveryException;
}
