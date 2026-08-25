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

package org.dromara.maxkey.notification.webhook;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationSink;
import org.dromara.maxkey.notification.core.spi.SinkDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通用 HTTP webhook sink：把主体变更事件 POST 给下游。
 *
 * <p>面向权限平台（permission-platform）的主体投影刷新端点设计，但不含任何
 * 平台专有类型 —— 仅依赖「URL + 令牌 + JSON body」这一通用形态，可复用于任意下游。
 *
 * <p>请求形态：
 * <pre>
 * POST {endpoint}
 * Authorization: Bearer {token}
 * X-Tenant-Id: {tenantId}
 * Idempotency-Key: {eventId}
 * Content-Type: application/json
 *
 * {"tenantId":"...","subjectType":"user","subjectId":"..."}
 * </pre>
 *
 * <p>失败分类：连接/读取超时与 5xx/429 视为可重试；其余 4xx 视为确定性失败。
 *
 * @author Avatar-Knowledge
 */
public final class WebhookNotificationSink implements ChangeNotificationSink {

    private static final Logger LOG = LoggerFactory.getLogger(WebhookNotificationSink.class);

    private final String sinkName;
    private final URI endpoint;
    private final String bearerToken;
    private final Duration requestTimeout;
    private final HttpClient httpClient;

    /**
     * @param sinkName       sink 名称，用于日志
     * @param endpoint       下游刷新端点完整 URL
     * @param bearerToken    调用令牌；为空则不带 Authorization 头
     * @param connectTimeout 连接超时
     * @param requestTimeout 单次请求总超时
     */
    public WebhookNotificationSink(String sinkName, String endpoint, String bearerToken,
                                   Duration connectTimeout, Duration requestTimeout) {
        if (sinkName == null || sinkName.trim().isEmpty()) {
            throw new IllegalArgumentException("sinkName is required");
        }
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("endpoint is required");
        }
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()
                || requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("positive connectTimeout and requestTimeout are required");
        }
        this.sinkName = sinkName.trim();
        this.endpoint = URI.create(endpoint.trim());
        this.bearerToken = bearerToken == null ? "" : bearerToken.trim();
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @Override
    public String name() {
        return sinkName;
    }

    @Override
    public void deliver(SubjectChangeEvent event) throws SinkDeliveryException {
        String body = renderBody(event);
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("X-Tenant-Id", event.getTenantId())
                .header("Idempotency-Key", event.getEventId())
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (!bearerToken.isEmpty()) {
            builder = builder.header("Authorization", "Bearer " + bearerToken);
        }

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException networkFailure) {
            throw SinkDeliveryException.transientFailure(
                    "webhook delivery failed: " + sinkName + " -> " + endpoint, networkFailure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw SinkDeliveryException.transientFailure(
                    "webhook delivery interrupted: " + sinkName, interrupted);
        }

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            LOG.debug("webhook accepted: sink={} status={} eventId={}", sinkName, status,
                    event.getEventId());
            return;
        }
        String detail = "sink=" + sinkName + " status=" + status + " body="
                + abbreviate(response.body());
        if (status == 429 || status >= 500) {
            throw SinkDeliveryException.transientFailure("webhook rejected (retryable): " + detail, null);
        }
        throw SinkDeliveryException.definitiveFailure("webhook rejected: " + detail, null);
    }

    /**
     * 渲染请求体。手写 JSON 而不引入序列化库，避免为一个三字段对象增加模块依赖。
     */
    private static String renderBody(SubjectChangeEvent event) {
        return "{\"tenantId\":\"" + escape(event.getTenantId())
                + "\",\"subjectType\":\"" + escape(event.getSubjectType().wireName())
                + "\",\"subjectId\":\"" + escape(event.getSubjectId()) + "\"}";
    }

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 8);
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            switch (current) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (current < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) current));
                    } else {
                        escaped.append(current);
                    }
            }
        }
        return escaped.toString();
    }

    private static String abbreviate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 200 ? body : body.substring(0, 200) + "...";
    }
}
