-- MaxKey 主体变更通知 - outbox 表
-- 与业务事务一起写入，保证事件不丢（outbox pattern）。
--
-- 投递状态：
--   PENDING   - 待投递
--   DELIVERED - 已投递成功
--   FAILED    - 重试耗尽，进入死信（需人工介入）
--
-- 索引策略：
--   (status, next_attempt_at) - relay 拉取待投递事件用
--   (entity_type, entity_id)  - 按实体去重/排查用

CREATE TABLE IF NOT EXISTS mxk_notification_outbox (
    id              VARCHAR(64)  NOT NULL COMMENT '事件 UUID',
    tenant_id       VARCHAR(64)  NOT NULL COMMENT '租户 id（已转换为下游格式）',
    subject_type    VARCHAR(32)  NOT NULL COMMENT '主体类型: USER/GROUP/ROLE',
    subject_id      VARCHAR(128) NOT NULL COMMENT '主体 id',
    operation       VARCHAR(16)  NOT NULL COMMENT '操作: CREATE/UPDATE/DELETE',
    source          VARCHAR(128)          COMMENT '事件来源（便于排查）',
    entity_type     VARCHAR(64)           COMMENT '触发事件的 IDM 实体类型',
    entity_id       VARCHAR(128)          COMMENT '触发事件的 IDM 实体 id',
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING' COMMENT '投递状态',
    retry_count     INT          NOT NULL DEFAULT 0 COMMENT '已重试次数',
    next_attempt_at DATETIME     NOT NULL COMMENT '下次可投递时间',
    last_error      VARCHAR(1024)         COMMENT '最后一次错误信息',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_status_next_attempt (status, next_attempt_at),
    KEY idx_entity (entity_type, entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='主体变更通知 outbox';
