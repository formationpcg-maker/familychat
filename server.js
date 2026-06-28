const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { randomUUID: uuidv4 } = require('crypto');
const Database = require('better-sqlite3');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 20e6 });

// ── SQLite setup ─────────────────────────────────────────────────────────────
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DB_PATH = path.join(DATA_DIR, 'chatounet.db');
const UPLOADS_DIR = path.join(__dirname, 'public', 'uploads');
fs.mkdirSync(UPLOADS_DIR, { recursive: true });

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    avatar TEXT,
    color TEXT
  );

  CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS conversation_members (
    conv_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (conv_id, user_id)
  );

  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    conv_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    text TEXT DEFAULT '',
    attachment TEXT,
    reactions TEXT DEFAULT '{}',
    ts INTEGER NOT NULL,
    read_by TEXT DEFAULT '[]'
  );
`);

// Seed initial data if empty
const userCount = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
if (userCount === 0) {
  const insertUser = db.prepare('INSERT INTO users (id, name, avatar, color) VALUES (?, ?, ?, ?)');
  const insertConv = db.prepare('INSERT INTO conversations (id, name, type) VALUES (?, ?, ?)');
  const insertMember = db.prepare('INSERT INTO conversation_members (conv_id, user_id) VALUES (?, ?)');

  const seedUsers = [
    { id: 'u1', name: 'Mat', avatar: '🧔', color: '#4A90D9' },
    { id: 'u2', name: 'Deb', avatar: '👩', color: '#E57373' },
  ];
  const seedConvs = [
    { id: 'group', name: 'Chatounet 💬', type: 'group', members: ['u1','u2'] },
    { id: 'u1-u2', name: null, type: 'dm', members: ['u1','u2'] },
  ];

  const seedTx = db.transaction(() => {
    seedUsers.forEach(u => insertUser.run(u.id, u.name, u.avatar, u.color));
    seedConvs.forEach(c => {
      insertConv.run(c.id, c.name, c.type);
      c.members.forEach(uid => insertMember.run(c.id, uid));
    });
  });
  seedTx();
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function getInitData() {
  const users = db.prepare('SELECT * FROM users').all();
  const convRows = db.prepare('SELECT * FROM conversations').all();
  const conversations = convRows.map(c => ({
    ...c,
    members: db.prepare('SELECT user_id FROM conversation_members WHERE conv_id = ?')
      .all(c.id).map(r => r.user_id),
  }));
  return { users, conversations };
}

function getMessages(convId) {
  return db.prepare('SELECT * FROM messages WHERE conv_id = ? ORDER BY ts ASC').all(convId)
    .map(m => ({
      ...m,
      convId: m.conv_id,
      senderId: m.sender_id,
      attachment: m.attachment ? JSON.parse(m.attachment) : null,
      reactions: JSON.parse(m.reactions),
      readBy: JSON.parse(m.read_by),
    }));
}

const insertMessage = db.prepare(
  'INSERT INTO messages (id, conv_id, sender_id, text, attachment, reactions, ts, read_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
);
const updateReactions = db.prepare('UPDATE messages SET reactions = ? WHERE id = ?');
const updateReadBy    = db.prepare('UPDATE messages SET read_by = ? WHERE id = ?');

// ── File upload ──────────────────────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: UPLOADS_DIR,
  filename: (req, file, cb) => cb(null, uuidv4() + path.extname(file.originalname)),
});
const upload = multer({ storage, limits: { fileSize: 20 * 1024 * 1024 } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── REST ─────────────────────────────────────────────────────────────────────
app.get('/api/init', (req, res) => res.json(getInitData()));

app.get('/api/messages/:convId', (req, res) => res.json(getMessages(req.params.convId)));

app.post('/api/avatar', (req, res) => {
  const { userId, avatar } = req.body;
  if (!userId || !avatar) return res.status(400).json({ error: 'Missing fields' });
  db.prepare('UPDATE users SET avatar = ? WHERE id = ?').run(avatar, userId);
  io.emit('avatarUpdate', { userId, avatar });
  res.json({ ok: true });
});

app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const isImage = req.file.mimetype.startsWith('image/');
  res.json({
    url: '/uploads/' + req.file.filename,
    name: req.file.originalname,
    type: isImage ? 'image' : 'file',
    size: req.file.size,
  });
});

// ── Socket.io ────────────────────────────────────────────────────────────────
const onlineUsers = new Map();

io.on('connection', (socket) => {
  socket.on('join', ({ userId }) => {
    onlineUsers.set(socket.id, userId);
    io.emit('presence', { userId, online: true });
    socket.emit('onlineList', [...onlineUsers.values()]);
  });

  socket.on('joinConv', ({ convId }) => socket.join(convId));

  socket.on('message', (msg) => {
    const full = {
      id: uuidv4(),
      convId: msg.convId,
      senderId: msg.senderId,
      text: msg.text || '',
      attachment: msg.attachment || null,
      reactions: {},
      ts: Date.now(),
      readBy: [msg.senderId],
    };
    insertMessage.run(
      full.id, full.convId, full.senderId, full.text,
      full.attachment ? JSON.stringify(full.attachment) : null,
      JSON.stringify(full.reactions),
      full.ts,
      JSON.stringify(full.readBy)
    );
    io.to(msg.convId).emit('message', full);
  });

  socket.on('react', ({ msgId, userId, emoji }) => {
    const row = db.prepare('SELECT reactions, conv_id FROM messages WHERE id = ?').get(msgId);
    if (!row) return;
    const reactions = JSON.parse(row.reactions);
    if (!reactions[emoji]) reactions[emoji] = [];
    const idx = reactions[emoji].indexOf(userId);
    if (idx === -1) reactions[emoji].push(userId);
    else reactions[emoji].splice(idx, 1);
    if (reactions[emoji].length === 0) delete reactions[emoji];
    updateReactions.run(JSON.stringify(reactions), msgId);
    io.to(row.conv_id).emit('reaction', { msgId, reactions });
  });

  socket.on('typing', ({ convId, userId, isTyping }) => {
    socket.to(convId).emit('typing', { userId, isTyping });
  });

  socket.on('markRead', ({ convId, userId }) => {
    const msgs = db.prepare('SELECT id, read_by FROM messages WHERE conv_id = ?').all(convId);
    const tx = db.transaction(() => {
      msgs.forEach(m => {
        const readBy = JSON.parse(m.read_by);
        if (!readBy.includes(userId)) {
          readBy.push(userId);
          updateReadBy.run(JSON.stringify(readBy), m.id);
        }
      });
    });
    tx();
    io.to(convId).emit('read', { convId, userId });
  });

  function toUser(userId, event, data) {
    for (const [sid, uid] of onlineUsers) {
      if (uid === userId) io.to(sid).emit(event, data);
    }
  }

  socket.on('call-offer',  ({ toUserId, fromUserId, offer })  => toUser(toUserId, 'call-offer',  { fromUserId, offer }));
  socket.on('call-answer', ({ toUserId, answer })              => toUser(toUserId, 'call-answer', { answer }));
  socket.on('call-ice',    ({ toUserId, candidate })           => toUser(toUserId, 'call-ice',    { candidate }));
  socket.on('call-end',    ({ toUserId })                      => toUser(toUserId, 'call-end',    {}));
  socket.on('call-reject', ({ toUserId })                      => toUser(toUserId, 'call-reject', {}));

  socket.on('disconnect', () => {
    const userId = onlineUsers.get(socket.id);
    onlineUsers.delete(socket.id);
    if (userId) io.emit('presence', { userId, online: false });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chatounet running on http://localhost:${PORT}`));
