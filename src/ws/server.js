import { WebSocketServer } from "ws";

function sendJson(socket, payload) {
    if(socket.readyState !== WebSocket.OPEN) return;

    socket.send(JSON.stringify(payload));
}

function broadcastToAll(wss, payload) {
    for (const client of wss.clients)  {
        if(client.readyState !== WebSocket.OPEN) continue;

        client.send(JSON.stringify(payload));
    }
}


export function createWSServer(app) {
    const wss = new WebSocketServer({ server: app });

    return wss;
}

export function attachWebSocketServer(server) {
    const wss = new WebSocketServer({ server , path: '/ws', maxPayload: 1024 * 1024 * 10 });

    wss.on('connection', (socket) => {
        sendJson(socket, { type: 'welcome' });
    });

    wss.on('upgrade', (request, socket, head) => {
        wss.handleUpgrade(request, socket, head, (socket) => {
            wss.emit('connection', socket, request);
        });
    });

    function broadcastMatchCreated(match) {
        broadcastToAll(wss, { type: 'match_created', data: match });
    }
    
    return {
        broadcastMatchCreated,
    }
    
}