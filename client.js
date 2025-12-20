const net = require('net');
const readline = require('readline');

// 创建客户端连接
const client = new net.Socket();
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

// 发送命令到 Redis 服务器
function sendCommand(command) {
    const args = command.split(' ');

    // 格式化命令为 Redis 协议格式
    let resp = `*${args.length}\r\n`;
    args.forEach(arg => {
        resp += `$${Buffer.byteLength(arg)}\r\n${arg}\r\n`;
    });

    console.log("发送的命令(十六进制):", Buffer.from(resp).toString('hex'));  // 用于调试
    client.write(resp);
}

// 连接到 Redis 服务器
client.connect(6379, 'localhost', () => {
    console.log('已连接到 Cjdis 服务器。输入命令或输入 "quit" 退出。');
    console.log("\n" +
        " ______       __     _____     __     ______    \n" +
        "/\\  ___\\     /\\ \\   /\\  __-.  /\\ \\   /\\  ___\\   \n" +
        "\\ \\ \\____   _\\_\\ \\  \\ \\ \\/\\ \\ \\ \\ \\  \\ \\___  \\  \n" +
        " \\ \\_____\\ /\\_____\\  \\ \\____-  \\ \\_\\  \\/\\_____\\ \n" +
        "  \\/_____/ \\/_____/   \\/____/   \\/_/   \\/_____/ \n" +
        "                                                \n")
    prompt();
});

// 解析 Redis 服务器响应
client.on('data', (data) => {
    const response = data.toString();
    console.log("收到原始响应:", response);

    // 处理不同类型的响应
    if (response.startsWith('+')) {
        // 简单字符串响应
        console.log("结果: ", response.substring(1).trim());
    } else if (response.startsWith('$')) {
        // 批量字符串响应
        const lines = response.split('\r\n');
        if (lines.length >= 2) {
            console.log("结果: ", lines[1]);
        }
    } else if (response.startsWith(':')) {
        // 整数响应
        console.log("结果: ", response.substring(1).trim());
    } else if (response.startsWith('*')) {
        // 数组响应
        const lines = response.split('\r\n');
        for (let i = 1; i < lines.length; i += 2) {
            if (lines[i] && !lines[i].startsWith('$')) {
                console.log("结果: ", lines[i]);
            }
        }
    } else if (response.startsWith('-')) {
        // 错误响应
        console.error("错误: ", response.substring(1).trim());
    } else {
        console.log("未知响应类型:", response);
    }

    prompt();
});

// 提示用户输入命令
function prompt() {
    rl.question('cjdis> ', (command) => {
        // 如果输入 'quit'，则关闭连接
        if (command.toLowerCase() === 'quit') {
            client.end();
            rl.close();
            return;
        }

        // 如果输入 'help'，则显示帮助信息
        if (command.toLowerCase() === 'help') {
            showHelp();
            prompt();
            return;
        }

        // 发送命令
        sendCommand(command);
    });
}

// 显示帮助信息
function showHelp() {
    console.log(`
    支持的命令:
    - help       显示帮助信息
    - quit       退出客户端
    - <其他 Redis 命令> 你可以执行 Redis 命令，比如 GET, SET 等。
    `);
}

// 连接关闭时的处理
client.on('close', () => {
    console.log('连接已关闭。感谢使用 Cjdis 客户端！');
    process.exit(0);
});

// 错误处理
client.on('error', (err) => {
    console.error('连接错误:', err);
    rl.close();
    process.exit(1);
});