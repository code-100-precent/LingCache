package utils

import (
	"strconv"
	"strings"
)

/*
 * ============================================================================
 * 配置管理
 * ============================================================================
 *
 * 提供类似 Redis 的配置方式
 * 从环境变量和 .env 文件读取配置
 */

// ServerConfig 服务器配置
type ServerConfig struct {
	// 服务器地址
	Addr string `env:"REDIS_ADDR"`

	// 数据库数量
	DbNum int `env:"REDIS_DB_NUM"`

	// RDB 文件路径
	RdbFilename string `env:"REDIS_RDB_FILENAME"`

	// AOF 文件路径
	AofFilename string `env:"REDIS_AOF_FILENAME"`

	// 是否启用 AOF
	AofEnabled bool `env:"REDIS_AOF_ENABLED"`

	// 是否启用 RDB
	RdbEnabled bool `env:"REDIS_RDB_ENABLED"`

	// 日志级别
	LogLevel string `env:"REDIS_LOG_LEVEL"`

	// 最大客户端连接数
	MaxClients int `env:"REDIS_MAX_CLIENTS"`

	// 慢查询日志阈值（毫秒）
	SlowLogThreshold int64 `env:"REDIS_SLOWLOG_THRESHOLD"`

	// 是否启用集群模式
	ClusterEnabled bool `env:"REDIS_CLUSTER_ENABLED"`

	// 集群端口
	ClusterPort int `env:"REDIS_CLUSTER_PORT"`

	// 集群节点 ID
	ClusterNodeID string `env:"REDIS_CLUSTER_NODE_ID"`
}

// LoadServerConfig 加载服务器配置
func LoadServerConfig() *ServerConfig {
	config := &ServerConfig{
		Addr:             GetEnvWithDefault("REDIS_ADDR", ":6379"),
		DbNum:            int(GetIntEnvWithDefault("REDIS_DB_NUM", 16)),
		RdbFilename:      GetEnvWithDefault("REDIS_RDB_FILENAME", "dump.rdb"),
		AofFilename:      GetEnvWithDefault("REDIS_AOF_FILENAME", "appendonly.aof"),
		AofEnabled:       GetBoolEnvWithDefault("REDIS_AOF_ENABLED", true),
		RdbEnabled:       GetBoolEnvWithDefault("REDIS_RDB_ENABLED", true),
		LogLevel:         GetEnvWithDefault("REDIS_LOG_LEVEL", "info"),
		MaxClients:       int(GetIntEnvWithDefault("REDIS_MAX_CLIENTS", 10000)),
		SlowLogThreshold: GetIntEnvWithDefault("REDIS_SLOWLOG_THRESHOLD", 10000),
		ClusterEnabled:   GetBoolEnvWithDefault("REDIS_CLUSTER_ENABLED", false),
		ClusterPort:      int(GetIntEnvWithDefault("REDIS_CLUSTER_PORT", 7000)),
		ClusterNodeID:    GetEnvWithDefault("REDIS_CLUSTER_NODE_ID", ""),
	}

	return config
}

// GetConfigValue 获取配置值（字符串）
func GetConfigValue(key string, defaultValue string) string {
	return GetEnvWithDefault(key, defaultValue)
}

// GetConfigInt 获取配置值（整数）
func GetConfigInt(key string, defaultValue int) int {
	return int(GetIntEnvWithDefault(key, int64(defaultValue)))
}

// GetConfigBool 获取配置值（布尔）
func GetConfigBool(key string, defaultValue bool) bool {
	return GetBoolEnvWithDefault(key, defaultValue)
}

// GetConfigFloat 获取配置值（浮点数）
func GetConfigFloat(key string, defaultValue float64) float64 {
	return GetFloatEnvWithDefault(key, defaultValue)
}

// ParseConfigInt 解析配置字符串为整数
func ParseConfigInt(value string, defaultValue int) int {
	if value == "" {
		return defaultValue
	}

	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}

	return int(val)
}

// ParseConfigBool 解析配置字符串为布尔值
func ParseConfigBool(value string, defaultValue bool) bool {
	if value == "" {
		return defaultValue
	}

	val, err := strconv.ParseBool(strings.ToLower(value))
	if err != nil {
		return defaultValue
	}

	return val
}
