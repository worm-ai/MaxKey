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

package org.dromara.maxkey.notification.idm.test;

import java.util.Map;

import org.dromara.maxkey.entity.idm.GroupMember;
import org.dromara.maxkey.notification.core.event.ChangeOperation;
import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.event.SubjectChangeType;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationPublisher;
import org.dromara.maxkey.persistence.service.GroupMemberService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;

/**
 * 联调测试端点：直接触发组成员变更，验证切面 → 分发器 → webhook → PP 整条链路。
 *
 * <p>⚠️ 仅用于联调验证，通过 {@code maxkey.notification.subject-change.test-endpoint=true}
 * 显式启用，默认关闭，生产切勿打开。
 *
 * <p>提供两种触发方式：
 * <ul>
 *   <li>{@code /notification-test/direct-publish}：直接走 publisher，跳过 Service/切面，
 *       用于单独验证 webhook + PP 端点联通性；</li>
 *   <li>{@code /notification-test/insert-member}：调 {@code groupMemberService.insert()}，
 *       走完整 Service → 切面 → 通知链路（验收用）。</li>
 * </ul>
 *
 * @author Avatar-Knowledge
 */
@RestController
@RequestMapping("/notification-test")
public class NotificationTestController {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationTestController.class);

    private final ChangeNotificationPublisher publisher;
    private final GroupMemberService groupMemberService;
    private final String defaultTenantId;

    public NotificationTestController(
            ChangeNotificationPublisher publisher,
            GroupMemberService groupMemberService,
            WebApplicationContext applicationContext,
            String defaultTenantId) {
        this.publisher = publisher;
        this.groupMemberService = groupMemberService;
        this.defaultTenantId = defaultTenantId;
        LOG.info("Notification test endpoint enabled (DEV ONLY, do not use in production)");
    }

    /**
     * 直接发布一个测试事件，验证 publisher → sink → 下游 的连通性。
     *
     * <p>请求体示例：
     * <pre>
     * {
     *   "tenantId": "erp-tenant",
     *   "subjectType": "USER",
     *   "subjectId": "622227747934179401",
     *   "operation": "UPDATE"
     * }
     * </pre>
     *
     * @param body 请求体
     * @return 简单确认消息
     */
    @PostMapping("/direct-publish")
    public Map<String, Object> directPublish(@RequestBody Map<String, String> body) {
        String tenantId = or(body.get("tenantId"), defaultTenantId);
        String subjectTypeStr = or(body.get("subjectType"), "USER");
        String subjectId = body.get("subjectId");
        String operationStr = or(body.get("operation"), "UPDATE");

        SubjectChangeType type = SubjectChangeType.valueOf(subjectTypeStr.toUpperCase());
        ChangeOperation op = ChangeOperation.valueOf(operationStr.toUpperCase());
        SubjectChangeEvent event = SubjectChangeEvent.of(tenantId, type, subjectId, op,
                "test:direct-publish");
        publisher.publish(event);
        LOG.info("Test direct-publish event dispatched: {}", event);
        return Map.of("status", "dispatched", "eventId", event.getEventId(),
                "note", "异步投递，请查下游日志或状态");
    }

    /**
     * 调 groupMemberService.insert()，走完整 Service → 切面 → 通知链路。
     *
     * <p>请求体示例：
     * <pre>
     * {
     *   "groupId": "622178518125510656",
     *   "groupName": "内部用户组",
     *   "memberId": "622227747934179401",
     *   "memberName": "孙尚香",
     *   "type": "USER",
     *   "instId": "1"
     * }
     * </pre>
     *
     * @param body 组成员属性
     * @return 插入结果
     */
    @PostMapping("/insert-member")
    public Map<String, Object> insertMember(@RequestBody Map<String, String> body) {
        LOG.info("groupMemberService bean class: {}", groupMemberService.getClass().getName());
        GroupMember member = new GroupMember(
                body.get("groupId"),
                body.get("groupName"),
                body.get("memberId"),
                body.get("memberName"),
                body.get("type"),
                body.get("instId"));
        member.setId(generateId(body.get("groupId"), body.get("memberId")));
        boolean success = groupMemberService.insert(member);
        LOG.info("Test insert-member: groupId={} memberId={} success={}",
                body.get("groupId"), body.get("memberId"), success);
        return Map.of("success", success,
                "note", "insert 已执行，如成功且通知链路正常，PP 投影应已自动刷新");
    }

    private static String or(String value, String fallback) {
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    /**
     * 生成一个稳定的测试用 id（group+member 组合），避免重复插入报主键冲突。
     */
    private static String generateId(String groupId, String memberId) {
        return "test-" + Math.abs((groupId + ":" + memberId).hashCode());
    }
}
