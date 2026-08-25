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

package org.dromara.maxkey.notification.idm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.dromara.maxkey.entity.idm.GroupMember;
import org.dromara.maxkey.notification.core.event.ChangeOperation;
import org.dromara.maxkey.notification.core.event.SubjectChangeEvent;
import org.dromara.maxkey.notification.core.event.SubjectChangeType;
import org.dromara.maxkey.notification.core.spi.ChangeNotificationPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 组成员变更切面：在 persistence 层拦一次，覆盖全部上游入口
 * （管理台 / 用户自助 / SCIM+REST API / 6 个同步器）。
 *
 * <p><b>为什么挂在 persistence 层而不是 Controller</b>：
 * 清点确认 MaxKey 共有 17 个 IDM 写入口分散在 4 个模块，逐个埋点必漏；
 * 且同步器是定时批量写，Controller 层根本不经过。persistence 层是唯一收敛点。
 *
 * <p><b>删除类的前置快照（关键正确性点）</b>：
 * {@code deleteBatch(ids)} 的参数是关系表主键，执行后无法反查 memberId，
 * 而「刚被移出组的用户」恰恰是最需要收回权限的主体。因此删除类必须在
 * {@code proceed()} 之前查出受影响用户，成功后再发事件。
 *
 * <p>阶段 1（最小闭环）只覆盖 GroupMember 的 insert / deleteBatch。
 * 其余写方法与 Roles/Organizations 在阶段 2 按文档 §3.2 白名单补全。
 *
 * @author Avatar-Knowledge
 */
@Aspect
public class GroupMemberChangeAspect {

    private static final Logger LOG = LoggerFactory.getLogger(GroupMemberChangeAspect.class);

    /** 成员类型：仅用户类成员才需要刷新用户主体投影。 */
    private static final Set<String> USER_MEMBER_TYPES = Set.of("USER", "USER-DYNAMIC");

    private final ChangeNotificationPublisher publisher;
    private final JdbcTemplate jdbcTemplate;
    private final String defaultTenantId;

    /**
     * @param publisher       变更发布入口
     * @param jdbcTemplate    用于删除前置快照查询（复用 MaxKey 自身数据源）
     * @param defaultTenantId 当实体 instId 缺失时使用的租户 id
     */
    public GroupMemberChangeAspect(ChangeNotificationPublisher publisher,
                                   JdbcTemplate jdbcTemplate,
                                   String defaultTenantId) {
        if (publisher == null || jdbcTemplate == null) {
            throw new IllegalArgumentException("publisher and jdbcTemplate are required");
        }
        this.publisher = publisher;
        this.jdbcTemplate = jdbcTemplate;
        this.defaultTenantId = defaultTenantId == null ? "" : defaultTenantId.trim();
    }

    /**
     * 拦截组成员新增。
     *
     * <p>注意：pointcut 用实现类而非接口，因为 {@code insert} 方法继承自
     * {@code IJpaService}，在 {@code GroupMemberService} 接口中没有显式声明，
     * AspectJ 基于接口签名的 execution 匹配不到。
     * bean 已是 CGLIB 代理（已验证为 $$SpringCGLIB$$0），拦实现类是生效的。
     */
    @Around("execution(* org.dromara.maxkey.persistence.service.impl.GroupMemberServiceImpl.insert(..))")
    public Object aroundInsert(ProceedingJoinPoint joinPoint) throws Throwable {
        Object result = joinPoint.proceed();
        if (!isSuccessful(result)) {
            return result;
        }
        try {
            for (Object argument : joinPoint.getArgs()) {
                if (argument instanceof GroupMember member) {
                    publishForMember(member, ChangeOperation.UPDATE, "GroupMemberServiceImpl.insert");
                }
            }
        } catch (RuntimeException notificationFailure) {
            // 通知失败绝不能影响业务写结果
            LOG.error("failed to publish group member insert notification", notificationFailure);
        }
        return result;
    }

    /**
     * 拦截组成员批量删除：{@code GroupMemberService.deleteBatch(List<String> ids)}。
     *
     * <p>必须前置快照：删除后无法从主键反查 memberId。
     */
    @Around("execution(* org.dromara.maxkey.persistence.service.GroupMemberService.deleteBatch(..))")
    public Object aroundDeleteBatch(ProceedingJoinPoint joinPoint) throws Throwable {
        List<AffectedMember> snapshot = List.of();
        try {
            snapshot = snapshotByRelationIds(joinPoint.getArgs());
        } catch (RuntimeException snapshotFailure) {
            LOG.error("failed to snapshot group members before delete; "
                    + "affected subjects will not be notified", snapshotFailure);
        }

        Object result = joinPoint.proceed();
        if (!isSuccessful(result)) {
            return result;
        }
        try {
            for (AffectedMember member : snapshot) {
                publish(member.tenantId(), member.memberId(), ChangeOperation.UPDATE,
                        "GroupMemberServiceImpl.deleteBatch");
            }
        } catch (RuntimeException notificationFailure) {
            LOG.error("failed to publish group member delete notification", notificationFailure);
        }
        return result;
    }

    /**
     * 用关系表主键反查受影响的用户成员（删除前调用）。
     */
    private List<AffectedMember> snapshotByRelationIds(Object[] args) {
        Collection<?> ids = extractIds(args);
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        Set<String> distinctIds = new LinkedHashSet<>();
        for (Object id : ids) {
            if (id != null && !id.toString().trim().isEmpty()) {
                distinctIds.add(id.toString().trim());
            }
        }
        if (distinctIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", distinctIds.stream().map(ignored -> "?").toList());
        String sql = "SELECT MEMBERID, TYPE, INSTID FROM mxk_group_member WHERE ID IN ("
                + placeholders + ")";
        List<AffectedMember> affected = new ArrayList<>();
        jdbcTemplate.query(sql, resultSet -> {
            String memberId = resultSet.getString("MEMBERID");
            String type = resultSet.getString("TYPE");
            String instId = resultSet.getString("INSTID");
            if (memberId != null && !memberId.trim().isEmpty() && isUserMember(type)) {
                affected.add(new AffectedMember(resolveTenantId(instId), memberId.trim()));
            }
        }, distinctIds.toArray());
        return affected;
    }

    private static Collection<?> extractIds(Object[] args) {
        if (args == null) {
            return null;
        }
        for (Object argument : args) {
            if (argument instanceof Collection<?> collection) {
                return collection;
            }
        }
        return null;
    }

    private void publishForMember(GroupMember member, ChangeOperation operation, String source) {
        if (!isUserMember(member.getType())) {
            // 组套组等非用户成员：阶段 1 不展开，留待阶段 2 处理嵌套关系
            LOG.debug("skip non-user group member notification: type={}", member.getType());
            return;
        }
        publish(resolveTenantId(member.getInstId()), member.getMemberId(), operation, source);
    }

    private void publish(String tenantId, String memberId, ChangeOperation operation, String source) {
        if (tenantId == null || tenantId.isEmpty() || memberId == null || memberId.trim().isEmpty()) {
            LOG.warn("skip subject change notification, missing tenantId or memberId: "
                    + "tenantId={} memberId={} source={}", tenantId, memberId, source);
            return;
        }
        publisher.publish(SubjectChangeEvent.of(tenantId, SubjectChangeType.USER,
                memberId.trim(), operation, source));
    }

    private static boolean isUserMember(String type) {
        return type == null || type.trim().isEmpty()
                || USER_MEMBER_TYPES.contains(type.trim().toUpperCase());
    }

    private String resolveTenantId(String instId) {
        if (instId != null && !instId.trim().isEmpty()) {
            return instId.trim();
        }
        return defaultTenantId;
    }

    /**
     * 判定业务方法是否执行成功。boolean 取其值，int 取 &gt;0，其余一律视为成功。
     */
    private static boolean isSuccessful(Object result) {
        if (result instanceof Boolean success) {
            return success;
        }
        if (result instanceof Number affectedRows) {
            return affectedRows.intValue() > 0;
        }
        return true;
    }

    /** 受影响的用户成员快照。 */
    private record AffectedMember(String tenantId, String memberId) {
    }
}
