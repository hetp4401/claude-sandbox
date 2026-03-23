const torrentStream = require('torrent-stream');

// Big Buck Bunny - well-seeded public domain torrent
const HASH = 'dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c';
const magnet = `magnet:?xt=urn:btih:${HASH}&tr=udp://tracker.opentrackr.org:1337/announce&tr=udp://open.stealth.si:80/announce&tr=udp://tracker.openbittorrent.com:6969/announce`;

console.log('Starting torrent-stream for Big Buck Bunny...');
console.log(`Magnet: ${magnet.slice(0, 80)}...`);
console.time('metadata');

const engine = torrentStream(magnet, {
  connections: 100,
  path: '/tmp/torrent-stream-test',
});

const timeout = setTimeout(() => {
  console.log('TIMEOUT after 5 minutes');
  engine.destroy();
  process.exit(1);
}, 5 * 60 * 1000);

engine.on('ready', () => {
  console.timeEnd('metadata');
  console.log(`\nGot ${engine.files.length} files:\n`);

  engine.files.forEach((file, idx) => {
    console.log(`  [${idx}] ${file.name} (${(file.length / 1024 / 1024).toFixed(2)} MB)`);
  });

  console.log('\nSUCCESS - torrent-stream resolved metadata!');
  clearTimeout(timeout);
  engine.destroy();
  process.exit(0);
});

engine.on('error', (err) => {
  console.error('Error:', err);
});
