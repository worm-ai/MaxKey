#!/bin/bash
# 用本地编译的新 jar 替换官方 maxkey-mgt 容器的 jar，commit 成新镜像
# 比 Dockerfile build 更直接：避免 arm64 下官方镜像 manifest 不匹配的问题
# 同时去掉官方镜像里的 sleep 60（开发调试用，生产镜像保留）
set -e

JAR_PATH="/Users/cc/projects/MaxKey/maxkey-webs/maxkey-web-mgt/build/libs/maxkey-mgt-boot-4.2.0-ga.jar"
NEW_IMAGE="maxkey-mgt:notify-20260825"
TMP_CONTAINER="maxkey-mgt-build-tmp"

echo ">>> 创建临时容器（官方镜像）"
docker rm -f "$TMP_CONTAINER" 2>/dev/null || true
docker create --name "$TMP_CONTAINER" maxkeytop/maxkey-mgt:latest sh > /dev/null

echo ">>> 拷贝新 jar 进容器"
docker cp "$JAR_PATH" "$TMP_CONTAINER:/maxkey-mgt/maxkey-mgt-boot.jar"

echo ">>> commit 为新镜像 $NEW_IMAGE（去掉 sleep 60，开发调试用）"
docker commit \
  -c 'CMD ["/bin/sh", "-c", "java -jar maxkey-mgt-boot.jar $JAVA_OPTS"]' \
  "$TMP_CONTAINER" "$NEW_IMAGE"

echo ">>> 清理临时容器"
docker rm -f "$TMP_CONTAINER"

echo ">>> 完成"
docker images | grep "maxkey-mgt"
