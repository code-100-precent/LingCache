package utils

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

/*
 * ============================================================================
 * .env 文件解析工具
 * ============================================================================
 *
 * 用于解析 .env 文件，支持环境变量配置
 * 类似 Redis 的配置方式，从环境变量和 .env 文件读取配置
 */

var (
	envCache map[string]string
	envMutex sync.RWMutex
	loaded   bool
)

func init() {
	envCache = make(map[string]string)
	loaded = false
}

// LoadEnv 加载 .env 文件
// env 参数用于指定环境（如 "dev", "prod"），对应 .env.dev, .env.prod
// 如果 env 为空，则加载 .env
func LoadEnv(env string) error {
	envMutex.Lock()
	defer envMutex.Unlock()

	// 确定文件名
	envFile := ".env"
	if env != "" {
		envFile = ".env." + env
	}

	// 读取文件
	data, err := os.ReadFile(envFile)
	if err != nil {
		// 如果文件不存在，不报错（允许只使用环境变量）
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// 解析文件内容
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// 跳过空行和注释
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// 解析 KEY=VALUE 格式
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// 移除引号（如果存在）
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}

		// 如果环境变量已存在，优先使用环境变量
		if _, exists := os.LookupEnv(key); !exists {
			// 设置到环境变量
			os.Setenv(key, value)
		}

		// 缓存到内存
		envCache[key] = value
	}

	loaded = true
	return nil
}

// GetEnv 获取环境变量值
// 优先从系统环境变量获取，其次从 .env 文件缓存获取
func GetEnv(key string) string {
	// 先尝试从系统环境变量获取
	if v := os.Getenv(key); v != "" {
		return v
	}

	// 从缓存获取
	envMutex.RLock()
	defer envMutex.RUnlock()

	if v, ok := envCache[key]; ok {
		return v
	}

	return ""
}

// GetEnvWithDefault 获取环境变量值，如果不存在则返回默认值
func GetEnvWithDefault(key, defaultValue string) string {
	if v := GetEnv(key); v != "" {
		return v
	}
	return defaultValue
}

// LookupEnv 查找环境变量，返回值和是否存在
func LookupEnv(key string) (value string, found bool) {
	// 先尝试从系统环境变量获取
	if v, ok := os.LookupEnv(key); ok {
		return v, true
	}

	// 从缓存获取
	envMutex.RLock()
	defer envMutex.RUnlock()

	if v, ok := envCache[key]; ok {
		return v, true
	}

	return "", false
}

// GetBoolEnv 获取布尔类型环境变量
func GetBoolEnv(key string) bool {
	v := GetEnv(key)
	if v == "" {
		return false
	}

	val, err := strconv.ParseBool(strings.ToLower(v))
	if err != nil {
		return false
	}

	return val
}

// GetBoolEnvWithDefault 获取布尔类型环境变量，带默认值
func GetBoolEnvWithDefault(key string, defaultValue bool) bool {
	v := GetEnv(key)
	if v == "" {
		return defaultValue
	}

	val, err := strconv.ParseBool(strings.ToLower(v))
	if err != nil {
		return defaultValue
	}

	return val
}

// GetIntEnv 获取整数类型环境变量
func GetIntEnv(key string) int64 {
	v := GetEnv(key)
	if v == "" {
		return 0
	}

	val, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}

	return val
}

// GetIntEnvWithDefault 获取整数类型环境变量，带默认值
func GetIntEnvWithDefault(key string, defaultValue int64) int64 {
	v := GetEnv(key)
	if v == "" {
		return defaultValue
	}

	val, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultValue
	}

	return val
}

// GetFloatEnv 获取浮点数类型环境变量
func GetFloatEnv(key string) float64 {
	v := GetEnv(key)
	if v == "" {
		return 0.0
	}

	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0.0
	}

	return val
}

// GetFloatEnvWithDefault 获取浮点数类型环境变量，带默认值
func GetFloatEnvWithDefault(key string, defaultValue float64) float64 {
	v := GetEnv(key)
	if v == "" {
		return defaultValue
	}

	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultValue
	}

	return val
}

// GetAllEnvs 获取所有环境变量（从缓存）
func GetAllEnvs() map[string]string {
	envMutex.RLock()
	defer envMutex.RUnlock()

	result := make(map[string]string)
	for k, v := range envCache {
		result[k] = v
	}

	return result
}

// IsLoaded 检查 .env 文件是否已加载
func IsLoaded() bool {
	envMutex.RLock()
	defer envMutex.RUnlock()
	return loaded
}

// Reload 重新加载 .env 文件
func Reload(env string) error {
	envMutex.Lock()
	envCache = make(map[string]string)
	loaded = false
	envMutex.Unlock()

	return LoadEnv(env)
}
