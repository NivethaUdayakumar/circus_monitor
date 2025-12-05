// Server/router.js
const fs   = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..', 'public'); // static files root

const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.csv':  'text/csv',
  '.png':  'image/png',
  '.svg':  'image/svg+xml'
};

function serveStatic(reqPath, res) {
  const filePath = path.join(ROOT, reqPath);
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404); res.end('Not found'); return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    fs.createReadStream(filePath).pipe(res);
  });
}

function handle(req, res, { projectCode }) {
  const reqPath = decodeURIComponent(req.url.split('?')[0]);

  if (reqPath === '/api/project-code') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ projectCode }));
    return;
  }

  if (reqPath === '/' || reqPath === '/index.html') {
    serveStatic('index.html', res);
    return;
  }

  serveStatic(reqPath.replace(/^\//, ''), res);
}

module.exports = { handle };
