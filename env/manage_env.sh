#!/bin/bash

# 环境管理wrapper脚本
# 自动发现并管理env目录下的所有docker compose项目

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_DIRS=("$SCRIPT_DIR"/*/)

# 检查docker compose是否可用
check_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    elif docker compose version &> /dev/null; then
        echo "docker compose"
    else
        echo "ERROR: Docker Compose not found" >&2
        exit 1
    fi
}

DOCKER_COMPOSE_CMD=$(check_docker_compose)

# 获取所有环境目录
get_env_dirs() {
    find "$SCRIPT_DIR" -maxdepth 1 -type d -name "*" ! -path "$SCRIPT_DIR" | sort
}

# 启动单个环境
start_env() {
    local env_dir="$1"
    local env_name="$(basename "$env_dir")"

    if [ -f "$env_dir/docker-compose.yml" ] || [ -f "$env_dir/docker-compose.yaml" ]; then
        echo "Starting $env_name..."
        (cd "$env_dir" && $DOCKER_COMPOSE_CMD up -d)
    fi
}

# 停止单个环境
stop_env() {
    local env_dir="$1"
    local env_name="$(basename "$env_dir")"

    if [ -f "$env_dir/docker-compose.yml" ] || [ -f "$env_dir/docker-compose.yaml" ]; then
        echo "Stopping $env_name..."
        (cd "$env_dir" && $DOCKER_COMPOSE_CMD down)
    fi
}

# 查看单个环境状态
status_env() {
    local env_dir="$1"
    local env_name="$(basename "$env_dir")"

    if [ -f "$env_dir/docker-compose.yml" ] || [ -f "$env_dir/docker-compose.yaml" ]; then
        echo "Status for $env_name:"
        (cd "$env_dir" && $DOCKER_COMPOSE_CMD ps)
        echo ""
    fi
}

# 启动所有环境
start_all() {
    echo "Starting all environments..."
    while IFS= read -r env_dir; do
        start_env "$env_dir"
    done < <(get_env_dirs)
    echo "All environments started"
}

# 停止所有环境
stop_all() {
    echo "Stopping all environments..."
    while IFS= read -r env_dir; do
        stop_env "$env_dir"
    done < <(get_env_dirs)
    echo "All environments stopped"
}

# 查看所有环境状态
status_all() {
    echo "Status for all environments:"
    while IFS= read -r env_dir; do
        status_env "$env_dir"
    done < <(get_env_dirs)
}

# 初始化网络
init_network() {
    local network_name="tough-development-domain"

    echo "Creating network: $network_name"
    docker network create \
        --driver bridge \
        --subnet 172.20.0.0/16 \
        --gateway 172.20.0.1 \
        "$network_name"
}

# 显示帮助
show_help() {
    cat << EOF
Usage: $0 <command> [environment]

Commands:
  start [env]     Start all environments or specific one
  stop [env]      Stop all environments or specific one
  status [env]    Show status of all environments or specific one
  initialize-network Create fixed tough-development-domain network
  help            Show this help message

Environments:
$(get_env_dirs | while read -r dir; do
    echo "  $(basename "$dir")"
done)

Examples:
  $0 start          # Start all environments
  $0 stop iotdb     # Stop only iotdb environment
  $0 status         # Show status of all environments
  $0 initialize-network  # Create tough-development-domain network
EOF
}

# 主逻辑
case "${1:-help}" in
    start)
        if [ -n "$2" ]; then
            start_env "$SCRIPT_DIR/$2"
        else
            start_all
        fi
        ;;
    stop)
        if [ -n "$2" ]; then
            stop_env "$SCRIPT_DIR/$2"
        else
            stop_all
        fi
        ;;
    status)
        if [ -n "$2" ]; then
            status_env "$SCRIPT_DIR/$2"
        else
            status_all
        fi
        ;;
    initialize-network)
        init_network
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
