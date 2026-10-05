const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const QRCode = require('qrcode');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const HOST_KEY = process.env.HOST_KEY || 'mech2026';

// Yerel ağ IP'sini al (192.168.x.x gibi)
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}
const LOCAL_IP = getLocalIP();

// ngrok tünel URL'ini al (ngrok çalışıyorsa)
async function getNgrokUrl() {
  try {
    const res = await fetch('http://localhost:4040/api/tunnels', { signal: AbortSignal.timeout(1000) });
    const data = await res.json();
    const tunnel = (data.tunnels || []).find(t => t.proto === 'https') || data.tunnels?.[0];
    return tunnel ? tunnel.public_url : null;
  } catch {
    return null;
  }
}

// En iyi public URL'i döndür: önce env var, sonra ngrok, sonra yerel IP
async function getPublicUrl() {
  if (process.env.PUBLIC_URL) return process.env.PUBLIC_URL.replace(/\/$/, '') + '/';
  const ngrok = await getNgrokUrl();
  if (ngrok) return ngrok + '/';
  return `http://${LOCAL_IP}:${PORT}/`;
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));
app.get('/mech', (_, res) => res.sendFile(path.join(__dirname, 'public', 'host.html')));

// QR endpoint — ngrok URL'i varsa onu, yoksa yerel IP kullan
app.get('/qr', async (req, res) => {
  try {
    const url = await getPublicUrl();
    res.json({ dataUrl: await QRCode.toDataURL(url, { margin: 1, width: 360 }), url });
  } catch (e) {
    res.status(500).end();
  }
});

// Oyun durumu
let questions = [];
try {
  questions = JSON.parse(
    fs.readFileSync(process.env.QUESTIONS_FILE || path.join(__dirname, 'questions.json'), 'utf8')
  );
} catch (e) {
  questions = [];
}

const game = {
  phase: 'lobby', // lobby | question | reveal | finished
  qIndex: -1,
  results: null,
  players: new Map() // token -> {name, score, guess, ws}
};

const send = (ws, msg) => {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
};

function leaderboard() {
  return [...game.players.values()]
    .map((p) => ({ name: p.name, score: p.score }))
    .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name, 'tr'));
}

function currentQuestion(forHost) {
  const q = questions[game.qIndex];
  if (!q) return null;
  const out = { text: q.text, unit: q.unit || '', index: game.qIndex, total: questions.length };
  if (forHost || game.phase === 'reveal' || game.phase === 'finished') out.answer = q.answer;
  return out;
}

function playerState(token) {
  const p = game.players.get(token);
  return {
    type: 'state',
    phase: game.phase,
    me: p ? { name: p.name, score: p.score, guess: p.guess } : null,
    playerCount: game.players.size,
    answeredCount: [...game.players.values()].filter((x) => x.guess !== null).length,
    question: currentQuestion(false),
    results: game.phase === 'reveal' ? game.results : null,
    leaderboard:
      game.phase === 'finished' || game.phase === 'reveal' ? leaderboard() : null
  };
}

function hostState() {
  return {
    type: 'hostState',
    phase: game.phase,
    questionCount: questions.length,
    question: currentQuestion(true),
    players: [...game.players.values()].map((p) => ({
      name: p.name,
      score: p.score,
      answered: p.guess !== null,
      online: !!p.ws && p.ws.readyState === 1
    })),
    results: game.phase === 'reveal' ? game.results : null,
    leaderboard: leaderboard(),
    questions: questions.map((q) => ({ text: q.text, answer: q.answer, unit: q.unit || '' }))
  };
}

let hostSocket = null;
function broadcast() {
  for (const [token, p] of game.players) send(p.ws, playerState(token));
  send(hostSocket, hostState());
}

function reveal() {
  const q = questions[game.qIndex];
  const guesses = [...game.players.values()].filter((p) => p.guess !== null);
  let best = Infinity;
  guesses.forEach((p) => (best = Math.min(best, Math.abs(p.guess - q.answer))));
  game.results = [...game.players.values()]
    .map((p) => {
      const diff = p.guess === null ? null : Math.abs(p.guess - q.answer);
      const winner = diff !== null && diff === best;
      if (winner) p.score += 1;
      return { name: p.name, guess: p.guess, diff, winner };
    })
    .sort((a, b) => (a.diff ?? Infinity) - (b.diff ?? Infinity));
  game.phase = 'reveal';
}

function resetGame() {
  game.phase = 'lobby';
  game.qIndex = -1;
  game.results = null;
  for (const p of game.players.values()) {
    p.score = 0;
    p.guess = null;
  }
}

wss.on('connection', (ws) => {
  let role = null;
  let token = null;

  ws.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }

    // HOST
    if (m.type === 'hostAuth') {
      if (m.key !== HOST_KEY) return send(ws, { type: 'hostDenied' });
      role = 'host';
      hostSocket = ws;
      return send(ws, hostState());
    }
    if (role === 'host') {
      if (m.type === 'start') {
        if (!questions.length || game.players.size === 0) return;
        game.qIndex = 0;
        for (const p of game.players.values()) p.guess = null;
        game.phase = 'question';
      } else if (m.type === 'reveal' && game.phase === 'question') {
        reveal();
      } else if (m.type === 'next' && game.phase === 'reveal') {
        if (game.qIndex + 1 >= questions.length) {
          game.phase = 'finished';
        } else {
          game.qIndex += 1;
          for (const p of game.players.values()) p.guess = null;
          game.results = null;
          game.phase = 'question';
        }
      } else if (m.type === 'restart') {
        resetGame();
      } else if (m.type === 'addQuestion' && game.phase === 'lobby') {
        const answer = Number(String(m.answer).replace(',', '.'));
        if (m.text && Number.isFinite(answer)) {
          questions.push({
            text: String(m.text).slice(0, 200),
            answer,
            unit: String(m.unit || '').slice(0, 20)
          });
        }
      } else if (m.type === 'removeQuestion' && game.phase === 'lobby') {
        if (Number.isInteger(m.index)) questions.splice(m.index, 1);
      } else if (m.type === 'kick' && game.phase === 'lobby') {
        for (const [t, p] of game.players) {
          if (p.name === m.name) {
            send(p.ws, { type: 'kicked' });
            game.players.delete(t);
          }
        }
      }
      return broadcast();
    }

    // OYUNCU
    if (m.type === 'join') {
      const name = String(m.name || '').trim().slice(0, 20);
      if (m.token && game.players.has(m.token)) {
        token = m.token;
        game.players.get(token).ws = ws;
        role = 'player';
        return broadcast();
      }
      if (!name) return send(ws, { type: 'joinError', message: 'İsim gerekli.' });
      if (game.phase !== 'lobby')
        return send(ws, { type: 'joinError', message: 'Oyun başladı, katılamazsın.' });
      const taken = [...game.players.values()].some(
        (p) => p.name.toLowerCase() === name.toLowerCase()
      );
      if (taken) return send(ws, { type: 'joinError', message: 'Bu isim alınmış.' });
      token = crypto.randomBytes(12).toString('hex');
      game.players.set(token, { name, score: 0, guess: null, ws });
      role = 'player';
      send(ws, { type: 'joined', token });
      return broadcast();
    }
    if (role === 'player' && m.type === 'guess' && game.phase === 'question') {
      const g = Number(String(m.value).replace(',', '.'));
      if (!Number.isFinite(g)) return;
      game.players.get(token).guess = g;
      return broadcast();
    }
  });

  ws.on('close', () => {
    if (role === 'host' && hostSocket === ws) hostSocket = null;
    if (role === 'player' && game.players.has(token)) game.players.get(token).ws = null;
    broadcast();
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('='.repeat(55));
  console.log(`  🎯 Yakın Tahmin Oyunu başlatıldı!`);
  console.log('='.repeat(55));
  console.log(`  Oyuncu girişi (bilgisayardan): http://localhost:${PORT}/`);
  console.log(`  Oyuncu girişi (telefondan QR): http://${LOCAL_IP}:${PORT}/`);
  console.log(`  Host paneli:  http://localhost:${PORT}/mech`);
  console.log(`  Host şifresi: mech2026`);
  console.log('='.repeat(55));
});
