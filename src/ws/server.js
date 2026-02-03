import {WebSocket, WebSocketServer} from 'ws';
import {wsArcjet} from "../arcjet.js";

const matchSubscribers = new Map();

/**
 * Ensure a subscriber set exists for the match and add the socket to that set.
 * @param {number} matchId - Numeric identifier of the match to subscribe to.
 * @param {WebSocket} socket - The client socket to add as a subscriber.
 */
function subscribe(matchId, socket) {
    if(!matchSubscribers.has(matchId)) {
        matchSubscribers.set(matchId, new Set());
    }

    matchSubscribers.get(matchId).add(socket);
}

/**
 * Remove a socket from the subscriber set for the given match and delete the match entry if it becomes empty.
 *
 * If there are no subscribers for the provided matchId, the function is a no-op.
 * @param {number} matchId - The identifier of the match whose subscriber list should be updated.
 * @param {WebSocket} socket - The subscriber socket to remove.
 */
function unsubscribe(matchId, socket) {
    const subscribers = matchSubscribers.get(matchId);

    if(!subscribers) return;

    subscribers.delete(socket);

    if(subscribers.size === 0) {
        matchSubscribers.delete(matchId);
    }
}

/**
 * Unsubscribes the given socket from every match it is currently subscribed to.
 * @param {import('ws')} socket - Socket object containing a `subscriptions` Set of matchId values to remove.
 */
function cleanupSubscriptions(socket) {
    for(const matchId of socket.subscriptions) {
        unsubscribe(matchId, socket);
    }
}

/**
 * Send a JSON-serializable payload to a WebSocket if the socket is open.
 *
 * If the socket is not in the OPEN state, the function returns without sending.
 *
 * @param {WebSocket} socket - The WebSocket to send the payload to.
 * @param {*} payload - The value to JSON-serialize and send. Must be serializable by JSON.stringify.
 */
function sendJson(socket, payload) {
    if(socket.readyState !== WebSocket.OPEN) return;

    socket.send(JSON.stringify(payload));
}

/**
 * Broadcast a JSON-serializable payload to every open client connected to the WebSocket server.
 *
 * @param {import('ws').Server} wss - The WebSocket server whose connected clients will receive the payload.
 * @param {*} payload - The value to JSON-serialize and send to each open client.
 */
function broadcastToAll(wss, payload) {
    for (const client of wss.clients)  {
        if(client.readyState !== WebSocket.OPEN) continue;

        client.send(JSON.stringify(payload));
    }
}

/**
 * Send a JSON-encoded payload to every open socket subscribed to a specific match.
 * @param {number} matchId - The match identifier whose subscribers should receive the payload.
 * @param {*} payload - A JSON-serializable value to send to each subscriber.
 */
function broadcastToMatch(matchId, payload) {
    const subscribers = matchSubscribers.get(matchId);
    if(!subscribers || subscribers.size === 0) return;

    const message = JSON.stringify(payload);

    for(const client of subscribers) {
        if(client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    }
}

/**
 * Handle an incoming WebSocket message, processing subscription and unsubscription commands and replying with status messages.
 *
 * Attempts to parse the raw `data` as JSON; if parsing fails, sends an error message `{ type: 'error', message: 'Invalid JSON' }`.
 * If the parsed message has `type: "subscribe"` and an integer `matchId`, subscribes the socket to that matchId, records the matchId in `socket.subscriptions`, and sends `{ type: 'subscribed', matchId }`.
 * If the parsed message has `type: "unsubscribe"` and an integer `matchId`, unsubscribes the socket from that matchId, removes the matchId from `socket.subscriptions`, and sends `{ type: 'unsubscribed', matchId }`.
 *
 * @param {WebSocket & { subscriptions?: Set<number> }} socket - The client's WebSocket connection; expected to have a `subscriptions` Set of matchId numbers.
 * @param {Buffer|string} data - Raw message payload received from the client.
 */
function handleMessage(socket, data) {
    let message;

    try {
        message = JSON.parse(data.toString());
    } catch {
        sendJson(socket, { type: 'error', message: 'Invalid JSON' });
    }

    if(message?.type === "subscribe" && Number.isInteger(message.matchId)) {
        subscribe(message.matchId, socket);
        socket.subscriptions.add(message.matchId);
        sendJson(socket, { type: 'subscribed', matchId: message.matchId });
        return;
    }

    if(message?.type === "unsubscribe" && Number.isInteger(message.matchId)) {
        unsubscribe(message.matchId, socket);
        socket.subscriptions.delete(message.matchId);
        sendJson(socket, { type: 'unsubscribed', matchId: message.matchId });
    }
}

export function attachWebSocketServer(server) {
    const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 * 10 });

    server.on('upgrade', async (request, socket, head) => {
        const { pathname } = new URL(request.url, `http://${request.headers.host}`);

        if (pathname !== '/ws') {
            socket.destroy();
            return;
        }

        if (wsArcjet) {
            try {
                const decision = await wsArcjet.protect(request);

                if (decision.isDenied()) {
                    if (decision.reason.isRateLimit()) {
                        socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');
                    } else {
                        socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
                    }
                    socket.destroy();
                    return;
                }
            } catch (e) {
                console.error('WS upgrade protection error', e);
                socket.write('HTTP/1.1 500 Internal Server Error\r\n\r\n');
                socket.destroy();
                return;
            }
        }

        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    });

    wss.on('connection', (socket) => {
        socket.isAlive = true;
        socket.on('pong', () => { socket.isAlive = true; });

        socket.subscriptions = new Set();

        sendJson(socket, { type: 'welcome' });

        socket.on('message', (data) => {
            handleMessage(socket, data);
        });

        socket.on('error', () => {
            socket.terminate();
        });

        socket.on('close', () => {
            cleanupSubscriptions(socket);
        })

        socket.on('error', console.error);
    });

    const interval = setInterval(() => {
        wss.clients.forEach((ws) => {
            if (ws.isAlive === false) {
                ws.terminate();
                return;
            }
    
            ws.isAlive = false;
            ws.ping();
        });
    }, 30000);

    wss.on('close', () => clearInterval(interval));

    function broadcastMatchCreated(match) {
        broadcastToAll(wss, { type: 'match_created', data: match });
    }

    function broadcastCommentary(matchId, comment) {
        broadcastToMatch(matchId, { type: 'commentary', data: comment });
    }
    
    return {
        broadcastMatchCreated,
        broadcastCommentary
    }
    
}