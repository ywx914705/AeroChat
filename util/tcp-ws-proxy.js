const net = require('net');
const WebSocket = require('ws');

const TCP_CONFIG = {
    HOST: '172.20.10.2',
    PORT: 8888
};

const wss = new WebSocket.Server({ port: 8080, host: '0.0.0.0' });

console.log('=== WebSocket中转服务启动 ===');
console.log(`监听地址：ws://0.0.0.0:8080`);
console.log(`转发到后端：${TCP_CONFIG.HOST}:${TCP_CONFIG.PORT}`);
console.log('==============================');

wss.on('connection', (ws) => {
    console.log(`[连接] 新客户端接入`);
    const tcpClient = new net.Socket();
    tcpClient.connect(TCP_CONFIG.PORT, TCP_CONFIG.HOST, () => {
        console.log('[TCP] 成功连接到后端服务');
    });

    // 前端 → 后端：原样转发，并自动追加换行符
    ws.on('message', (msg) => {
        const msgStr = msg.toString().trim();
        console.log(`[转发] 前端→后端：${msgStr}`);
        tcpClient.write(msgStr + '\n');
    });

    // 后端 → 前端：原样转发
    tcpClient.on('data', (data) => {
        const dataStr = data.toString().trim();
        console.log(`[转发] 后端→前端：${dataStr}`);
        ws.send(dataStr);
    });

    // 任一连接关闭时，清理另一个连接
    ws.on('close', () => {
        console.log(`[断开] WebSocket 客户端退出`);
        tcpClient.destroy();
    });

    ws.on('error', (err) => {
        console.error('[WS错误]', err.message);
        tcpClient.destroy();
    });

    tcpClient.on('close', () => {
        console.log('[断开] TCP 连接关闭');
        ws.close();
    });

    tcpClient.on('error', (err) => {
        console.error('[TCP错误]', err.message);
        ws.close();
    });
});