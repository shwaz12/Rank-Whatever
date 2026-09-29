// Rank Whatever: snake draft rooms.
// Each room is saved in Upstash Redis. Connect it in Vercel: your project -> Storage -> Upstash for Redis.
const crypto = require('crypto');

const DB_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const DB_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL = 60 * 60 * 72; // rooms disappear after 3 days
const MAX_ITEMS = 400;
const MAX_PLAYERS = 24;

class HttpError extends Error { constructor(status, msg) { super(msg); this.status = status; } }
const fail = (status, msg) => { throw new HttpError(status, msg); };

async function redis(cmd) {
  const r = await fetch(DB_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${DB_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd)
  });
  const j = await r.json().catch(() => ({ error: 'bad response' }));
  if (!r.ok || j.error) throw new HttpError(500, 'Database error: ' + (j.error || r.status));
  return j.result;
}

const rid = n => crypto.randomBytes(n).toString('base64url').slice(0, n);
const LETTERS = 'ABCDEFGHJKMNPQRSTUVWXYZ';
function newCode() { let s = ''; for (const x of crypto.randomBytes(5)) s += LETTERS[x % LETTERS.length]; return s; }
const cleanCode = c => String(c || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 8);
const cleanName = n => String(n || '').replace(/\s+/g, ' ').trim().slice(0, 24);

function pub(s) { const { keys, hostKey, ...rest } = s; return rest; }
function clockAt(s, idx) {
  const n = s.order.length; if (!n) return null;
  const r = Math.floor(idx / n), pos = idx % n;
  return r % 2 === 0 ? s.order[pos] : s.order[n - 1 - pos];
}
const totalPicks = s => Math.min(s.rounds * s.order.length, s.items.length);

async function load(code) { const raw = await redis(['GET', 'draft:' + code]); return raw ? JSON.parse(raw) : null; }
async function save(s) {
  s.v = (s.v || 0) + 1; s.updatedAt = Date.now();
  await redis(['SET', 'draft:' + s.code, JSON.stringify(s), 'EX', String(TTL)]);
}

async function withLock(code, fn) {
  const key = 'lock:draft:' + code, tok = rid(10);
  for (let i = 0; i < 30; i++) {
    if (await redis(['SET', key, tok, 'NX', 'PX', '5000']) === 'OK') {
      try { return await fn(); }
      finally {
        await redis(['EVAL', "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end", '1', key, tok]).catch(() => {});
      }
    }
    await new Promise(r => setTimeout(r, 100));
  }
  fail(503, 'The room is busy, try again');
}

function removePlayer(s, id) {
  if (!s.keys[id]) fail(404, 'That player is not here');
  s.players = s.players.filter(p => p.id !== id);
  s.order = s.order.filter(x => x !== id);
  delete s.keys[id];
}

function act(s, b) {
  const isHost = !!b.hostKey && b.hostKey === s.hostKey;
  const needHost = () => { if (!isHost) fail(403, 'Only the host can do that'); };
  const needLobby = () => { if (s.status !== 'lobby') fail(400, 'The draft already started'); };

  switch (b.action) {
    case 'join': {
      needLobby();
      const name = cleanName(b.name);
      if (!name) fail(400, 'Type your name first');
      if (s.players.length >= MAX_PLAYERS) fail(400, 'This draft is full');
      if (s.players.some(p => p.name.toLowerCase() === name.toLowerCase())) fail(400, 'Someone already has that name');
      const id = rid(8), key = rid(20);
      s.players.push({ id, name }); s.order.push(id); s.keys[id] = key;
      return { me: { playerId: id, key } };
    }
    case 'leave': {
      needLobby();
      if (!b.playerId || s.keys[b.playerId] !== b.key) fail(403, 'Not you');
      if (b.playerId === s.hostId) fail(400, "The host can't leave");
      removePlayer(s, b.playerId);
      return {};
    }
    case 'kick': {
      needHost(); needLobby();
      if (b.playerId === s.hostId) fail(400, "You can't kick yourself");
      removePlayer(s, String(b.playerId || ''));
      return {};
    }
    case 'order': {
      needHost(); needLobby();
      const o = Array.isArray(b.order) ? b.order.map(String) : [];
      if (o.length !== s.order.length || new Set(o).size !== o.length || !o.every(id => s.keys[id])) fail(400, 'Bad order');
      s.order = o;
      return {};
    }
    case 'shuffle': {
      needHost(); needLobby();
      const a = s.order;
      for (let i = a.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [a[i], a[j]] = [a[j], a[i]]; }
      return {};
    }
    case 'settings': {
      needHost(); needLobby();
      if ('rounds' in b) { const r = parseInt(b.rounds, 10); s.rounds = r > 0 ? Math.min(r, MAX_ITEMS) : null; }
      if ('title' in b) s.title = String(b.title || '').trim().slice(0, 100);
      return {};
    }
    case 'start': {
      needHost(); needLobby();
      const n = s.order.length;
      if (!n) fail(400, 'Nobody has joined yet');
      const max = Math.floor(s.items.length / n);
      if (max < 1) fail(400, `You need at least ${n} names for ${n} players`);
      s.rounds = s.rounds ? Math.min(s.rounds, max) : max;
      s.status = 'drafting'; s.startedAt = Date.now();
      return {};
    }
    case 'pick': {
      if (s.status !== 'drafting') fail(400, 'The draft is not running');
      if (typeof b.at === 'number' && b.at !== s.picks.length) fail(409, 'Someone just picked. Try again');
      const cur = clockAt(s, s.picks.length);
      const mine = b.playerId === cur && !!s.keys[cur] && s.keys[cur] === b.key;
      if (!mine && !isHost) fail(403, "It's not your turn");
      const itemId = String(b.itemId || '');
      if (!s.items.some(it => it.id === itemId)) fail(400, 'That name is not in this draft');
      if (s.picks.some(p => p.i === itemId)) fail(409, 'Too slow! That one was just taken');
      s.picks.push({ p: cur, i: itemId, at: Date.now() });
      if (s.picks.length >= totalPicks(s)) { s.status = 'done'; s.endedAt = Date.now(); }
      return {};
    }
    case 'undo': {
      needHost();
      if (s.status === 'lobby' || !s.picks.length) fail(400, 'Nothing to undo');
      s.picks.pop(); s.status = 'drafting';
      return {};
    }
    default:
      fail(400, 'Unknown action');
  }
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!DB_URL || !DB_TOKEN) return res.status(500).json({ error: 'NO_DB' });
  try {
    if (req.method === 'GET') {
      const code = cleanCode(req.query && req.query.room);
      const s = code && await load(code);
      if (!s) return res.status(404).json({ error: "That room doesn't exist (or it expired)" });
      return res.status(200).json({ state: pub(s) });
    }
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

    let b = req.body;
    if (typeof b === 'string') { try { b = JSON.parse(b || '{}'); } catch (_) { b = {}; } }
    b = b || {};

    if (b.action === 'create') {
      const name = cleanName(b.name);
      if (!name) fail(400, 'Type your name first');
      const seen = new Set(), items = [];
      for (const raw of (Array.isArray(b.items) ? b.items : [])) {
        const t = String(raw || '').replace(/\s+/g, ' ').trim().slice(0, 80);
        if (!t || seen.has(t.toLowerCase())) continue;
        seen.add(t.toLowerCase());
        items.push({ id: 'i' + items.length.toString(36), t });
        if (items.length >= MAX_ITEMS) break;
      }
      if (items.length < 2) fail(400, 'Add at least 2 names to draft');
      const id = rid(8), key = rid(20), hostKey = rid(20), now = Date.now();
      const s = {
        code: '', title: String(b.title || '').trim().slice(0, 100), items, status: 'lobby',
        players: [{ id, name }], order: [id], hostId: id, rounds: null, picks: [],
        createdAt: now, updatedAt: now, v: 1, keys: { [id]: key }, hostKey
      };
      for (let i = 0; i < 8; i++) {
        s.code = newCode();
        if (await redis(['SET', 'draft:' + s.code, JSON.stringify(s), 'NX', 'EX', String(TTL)]) === 'OK') {
          return res.status(200).json({ state: pub(s), me: { playerId: id, key, hostKey } });
        }
      }
      fail(500, "Couldn't make a room, try again");
    }

    const code = cleanCode(b.room);
    if (!code) fail(400, 'Missing room code');
    const out = await withLock(code, async () => {
      const s = await load(code);
      if (!s) fail(404, "That room doesn't exist (or it expired)");
      const extra = act(s, b);
      await save(s);
      return { state: pub(s), ...extra };
    });
    return res.status(200).json(out);
  } catch (e) {
    return res.status(e.status || 500).json({ error: e.status ? e.message : 'Server error' });
  }
};
