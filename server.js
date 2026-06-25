const http = require('http')
const WebSocket = require('ws')

const port = parseInt(process.env.PORT || '4444')
const topics = new Map()

const send = (conn, message) => {
  if (conn.readyState !== WebSocket.CONNECTING && conn.readyState !== WebSocket.OPEN) {
    conn.close(); return
  }
  try { conn.send(JSON.stringify(message)) } catch (_) { conn.close() }
}

const onconnection = (conn) => {
  const subscribedTopics = new Set()
  let closed = false, pongReceived = true

  const pingInterval = setInterval(() => {
    if (!pongReceived) { conn.close(); clearInterval(pingInterval) }
    else { pongReceived = false; try { conn.ping() } catch (_) { conn.close() } }
  }, 30000)

  conn.on('pong', () => { pongReceived = true })
  conn.on('close', () => {
    subscribedTopics.forEach((t) => {
      const subs = topics.get(t)
      if (subs) { subs.delete(conn); if (subs.size === 0) topics.delete(t) }
    })
    subscribedTopics.clear(); closed = true; clearInterval(pingInterval)
  })
  conn.on('message', (data) => {
    let msg
    try { msg = JSON.parse(typeof data === 'string' ? data : Buffer.from(data).toString()) }
    catch (_) { return }
    if (!msg || !msg.type || closed) return
    switch (msg.type) {
      case 'subscribe':
        ;(msg.topics || []).forEach((t) => {
          if (typeof t !== 'string') return
          if (!topics.has(t)) topics.set(t, new Set())
          topics.get(t).add(conn); subscribedTopics.add(t)
        }); break
      case 'unsubscribe':
        ;(msg.topics || []).forEach((t) => { const s = topics.get(t); if (s) s.delete(conn) }); break
      case 'publish':
        if (msg.topic) {
          const receivers = topics.get(msg.topic)
          if (receivers) { msg.clients = receivers.size; receivers.forEach((r) => send(r, msg)) }
        }; break
      case 'ping': send(conn, { type: 'pong' }); break
    }
  })
}

const server = http.createServer((_req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' })
  res.end('y-webrtc signaling\n')
})

const wss = new WebSocket.Server({ server })
wss.on('connection', onconnection)
server.listen(port, () => console.log(`Signaling server on port ${port}`))
