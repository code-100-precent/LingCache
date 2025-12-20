package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/code-100-precent/LingCache/server"
	"github.com/code-100-precent/LingCache/utils"
)

func main() {
	// 加载 .env 文件
	env := os.Getenv("ENV")
	if env == "" {
		env = "dev"
	}

	if err := utils.LoadEnv(env); err != nil {
		fmt.Printf("Warning: Failed to load .env file: %v\n", err)
	}

	// 加载配置
	config := utils.LoadServerConfig()

	// 命令行参数（优先级高于 .env）
	addr := flag.String("addr", config.Addr, "Server address")
	dbnum := flag.Int("dbnum", config.DbNum, "Number of databases")
	flag.Parse()

	// 创建服务器
	srv := server.NewServer(*addr, *dbnum)

	// 初始化 AOF（如果启用）
	if err := srv.InitAOF(config.AofEnabled, config.AofFilename); err != nil {
		fmt.Printf("Warning: Failed to initialize AOF: %v\n", err)
	}

	// 初始化集群（如果启用）
	if config.ClusterEnabled {
		clusterAddr := fmt.Sprintf("%s", *addr)
		if config.ClusterPort > 0 {
			clusterAddr = fmt.Sprintf(":%d", config.ClusterPort)
		}
		if err := srv.InitCluster(true, config.ClusterNodeID, clusterAddr); err != nil {
			fmt.Printf("Warning: Failed to initialize cluster: %v\n", err)
		}
	}

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 启动服务器（在 goroutine 中）
	go func() {
		if err := srv.Start(); err != nil {
			fmt.Printf("Server error: %v\n", err)
			os.Exit(1)
		}
	}()

	fmt.Printf("LingCache server started on %s\n", *addr)
	fmt.Printf("Database number: %d\n", *dbnum)
	fmt.Printf("RDB enabled: %v\n", config.RdbEnabled)
	fmt.Printf("AOF enabled: %v\n", config.AofEnabled)
	if config.ClusterEnabled {
		fmt.Printf("Cluster mode: enabled (port: %d)\n", config.ClusterPort)
	}

	// 等待信号
	<-sigChan
	fmt.Println("\nShutting down server...")
	srv.Stop()
	fmt.Println("Server stopped")
}
