package utils

import (
	"os"
	"testing"
)

func TestLoadEnv(t *testing.T) {
	// 创建测试 .env 文件
	testEnvContent := `
# Test comment
TEST_KEY=test_value
TEST_INT=123
TEST_BOOL=true
TEST_FLOAT=3.14
TEST_STRING="quoted string"
TEST_STRING2='single quoted'
`

	err := os.WriteFile(".env.test", []byte(testEnvContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test .env file: %v", err)
	}
	defer os.Remove(".env.test")

	// 加载测试环境
	err = LoadEnv("test")
	if err != nil {
		t.Fatalf("Failed to load .env file: %v", err)
	}

	// 测试获取值
	if v := GetEnv("TEST_KEY"); v != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", v)
	}

	if v := GetIntEnv("TEST_INT"); v != 123 {
		t.Errorf("Expected 123, got %d", v)
	}

	if v := GetBoolEnv("TEST_BOOL"); v != true {
		t.Errorf("Expected true, got %v", v)
	}

	if v := GetFloatEnv("TEST_FLOAT"); v != 3.14 {
		t.Errorf("Expected 3.14, got %f", v)
	}

	if v := GetEnv("TEST_STRING"); v != "quoted string" {
		t.Errorf("Expected 'quoted string', got '%s'", v)
	}

	if v := GetEnv("TEST_STRING2"); v != "single quoted" {
		t.Errorf("Expected 'single quoted', got '%s'", v)
	}
}

func TestGetEnvWithDefault(t *testing.T) {
	// 测试不存在的键
	if v := GetEnvWithDefault("NON_EXISTENT_KEY", "default"); v != "default" {
		t.Errorf("Expected 'default', got '%s'", v)
	}

	// 测试存在的键
	os.Setenv("EXISTING_KEY", "existing_value")
	if v := GetEnvWithDefault("EXISTING_KEY", "default"); v != "existing_value" {
		t.Errorf("Expected 'existing_value', got '%s'", v)
	}
	os.Unsetenv("EXISTING_KEY")
}

func TestGetIntEnvWithDefault(t *testing.T) {
	if v := GetIntEnvWithDefault("NON_EXISTENT_INT", 999); v != 999 {
		t.Errorf("Expected 999, got %d", v)
	}

	os.Setenv("EXISTING_INT", "456")
	if v := GetIntEnvWithDefault("EXISTING_INT", 999); v != 456 {
		t.Errorf("Expected 456, got %d", v)
	}
	os.Unsetenv("EXISTING_INT")
}

func TestGetBoolEnvWithDefault(t *testing.T) {
	if v := GetBoolEnvWithDefault("NON_EXISTENT_BOOL", true); v != true {
		t.Errorf("Expected true, got %v", v)
	}

	os.Setenv("EXISTING_BOOL", "false")
	if v := GetBoolEnvWithDefault("EXISTING_BOOL", true); v != false {
		t.Errorf("Expected false, got %v", v)
	}
	os.Unsetenv("EXISTING_BOOL")
}

func TestLookupEnv(t *testing.T) {
	// 测试不存在的键
	_, found := LookupEnv("NON_EXISTENT")
	if found {
		t.Error("Expected false, got true")
	}

	// 测试存在的键
	os.Setenv("EXISTING", "value")
	value, found := LookupEnv("EXISTING")
	if !found {
		t.Error("Expected true, got false")
	}
	if value != "value" {
		t.Errorf("Expected 'value', got '%s'", value)
	}
	os.Unsetenv("EXISTING")
}
