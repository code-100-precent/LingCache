# Utils 工具包

## 功能说明

`utils` 包提供了 `.env` 文件解析和配置管理功能，类似 Redis 的配置方式。

## 主要功能

### 1. 环境变量解析

- **LoadEnv(env string)** - 加载 `.env` 文件
  - `env` 参数用于指定环境（如 "dev", "prod"），对应 `.env.dev`, `.env.prod`
  - 如果 `env` 为空，则加载 `.env`
  - 支持注释（以 `#` 开头）
  - 支持引号（单引号和双引号）
  - 优先使用系统环境变量

- **GetEnv(key string)** - 获取环境变量值
- **GetEnvWithDefault(key, defaultValue string)** - 获取环境变量值，带默认值
- **LookupEnv(key string)** - 查找环境变量，返回值和是否存在

### 2. 类型转换

- **GetBoolEnv(key string)** - 获取布尔类型环境变量
- **GetBoolEnvWithDefault(key string, defaultValue bool)** - 获取布尔类型环境变量，带默认值
- **GetIntEnv(key string)** - 获取整数类型环境变量
- **GetIntEnvWithDefault(key string, defaultValue int64)** - 获取整数类型环境变量，带默认值
- **GetFloatEnv(key string)** - 获取浮点数类型环境变量
- **GetFloatEnvWithDefault(key string, defaultValue float64)** - 获取浮点数类型环境变量，带默认值

### 3. 配置管理

- **LoadServerConfig()** - 加载服务器配置
- **GetConfigValue(key, defaultValue string)** - 获取配置值（字符串）
- **GetConfigInt(key string, defaultValue int)** - 获取配置值（整数）
- **GetConfigBool(key string, defaultValue bool)** - 获取配置值（布尔）
- **GetConfigFloat(key string, defaultValue float64)** - 获取配置值（浮点数）

## 使用示例

### 1. 基本使用

```go
package main

import (
    "github.com/code-100-precent/LingCache/utils"
)

func main() {
    // 加载 .env 文件
    utils.LoadEnv("dev") // 加载 .env.dev
    
    // 获取环境变量
    addr := utils.GetEnv("REDIS_ADDR")
    dbNum := utils.GetIntEnv("REDIS_DB_NUM")
    enabled := utils.GetBoolEnv("REDIS_AOF_ENABLED")
}
```

### 2. 使用配置结构

```go
package main

import (
    "github.com/code-100-precent/LingCache/utils"
)

func main() {
    // 加载 .env 文件
    utils.LoadEnv("")
    
    // 加载服务器配置
    config := utils.LoadServerConfig()
    
    // 使用配置
    fmt.Printf("Server address: %s\n", config.Addr)
    fmt.Printf("Database number: %d\n", config.DbNum)
}
```

### 3. .env 文件格式

创建 `.env` 文件：

```env
# Server Configuration
REDIS_ADDR=:6379
REDIS_DB_NUM=16

# Persistence Configuration
REDIS_RDB_FILENAME=dump.rdb
REDIS_RDB_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
REDIS_AOF_ENABLED=true

# Logging Configuration
REDIS_LOG_LEVEL=info

# Connection Configuration
REDIS_MAX_CLIENTS=10000

# Performance Configuration
REDIS_SLOWLOG_THRESHOLD=10000

# Cluster Configuration
REDIS_CLUSTER_ENABLED=false
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=
```

## 配置项说明

### 服务器配置

- `REDIS_ADDR` - 服务器监听地址（默认：`:6379`）
- `REDIS_DB_NUM` - 数据库数量（默认：`16`）

### 持久化配置

- `REDIS_RDB_FILENAME` - RDB 文件路径（默认：`dump.rdb`）
- `REDIS_RDB_ENABLED` - 是否启用 RDB（默认：`true`）
- `REDIS_AOF_FILENAME` - AOF 文件路径（默认：`appendonly.aof`）
- `REDIS_AOF_ENABLED` - 是否启用 AOF（默认：`true`）

### 日志配置

- `REDIS_LOG_LEVEL` - 日志级别（默认：`info`）

### 连接配置

- `REDIS_MAX_CLIENTS` - 最大客户端连接数（默认：`10000`）

### 性能配置

- `REDIS_SLOWLOG_THRESHOLD` - 慢查询日志阈值（毫秒，默认：`10000`）

### 集群配置

- `REDIS_CLUSTER_ENABLED` - 是否启用集群模式（默认：`false`）
- `REDIS_CLUSTER_PORT` - 集群端口（默认：`7000`）
- `REDIS_CLUSTER_NODE_ID` - 集群节点 ID

## 特性

1. **优先级**：系统环境变量 > .env 文件
2. **缓存**：解析结果会缓存到内存，提高性能
3. **线程安全**：使用读写锁保护缓存
4. **类型转换**：自动处理字符串到各种类型的转换
5. **默认值**：所有获取函数都支持默认值

## 注意事项

1. `.env` 文件不存在时不会报错（允许只使用环境变量）
2. 注释以 `#` 开头
3. 支持单引号和双引号包裹的值
4. 键名不区分大小写（统一转换为大写）

