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

/**
 * sink 投递失败。
 *
 * <p>{@link #isTransient()} 区分「可重试」与「确定性失败」：
 * <ul>
 *   <li>可重试：网络超时、5xx、连接被拒 —— 分发器会退避重试；</li>
 *   <li>确定性失败：4xx（除 429）、协议错误 —— 重试无意义，直接放弃并告警。</li>
 * </ul>
 *
 * @author Avatar-Knowledge
 */
public class SinkDeliveryException extends Exception {

    private static final long serialVersionUID = 1L;

    private final boolean transientFailure;

    private SinkDeliveryException(String message, Throwable cause, boolean transientFailure) {
        super(message, cause);
        this.transientFailure = transientFailure;
    }

    /**
     * 构造一个可重试失败。
     *
     * @param message 失败描述
     * @param cause   原始异常，可为 null
     * @return 异常实例
     */
    public static SinkDeliveryException transientFailure(String message, Throwable cause) {
        return new SinkDeliveryException(message, cause, true);
    }

    /**
     * 构造一个确定性失败（重试无意义）。
     *
     * @param message 失败描述
     * @param cause   原始异常，可为 null
     * @return 异常实例
     */
    public static SinkDeliveryException definitiveFailure(String message, Throwable cause) {
        return new SinkDeliveryException(message, cause, false);
    }

    /**
     * 是否为可重试的瞬时失败。
     *
     * @return true 表示可重试
     */
    public boolean isTransient() {
        return transientFailure;
    }
}
