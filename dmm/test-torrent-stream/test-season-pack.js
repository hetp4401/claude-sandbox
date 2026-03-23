const torrentStream = require('torrent-stream');

// Roseanne Season 6 - actual season pack from our DB
const HASH = '02c04ebd0f409a94ef6e99a5bbf06389ee33a348';
const magnet = `magnet:?xt=urn:btih:${HASH}&tr=udp://tracker.opentrackr.org:1337/announce&tr=udp://open.stealth.si:80/announce&tr=udp://tracker.openbittorrent.com:6969/announce&tr=udp://exodus.desync.com:6969/announce`;

console.log(`Testing season pack: Roseanne S06`);
console.log(`Hash: ${HASH}`);
console.time('metadata');

const engine = torrentStream(magnet, {
  connections: 100,
  path: '/tmp/torrent-stream-test',
});

const timeout = setTimeout(() => {
  console.log('TIMEOUT after 3 minutes');
  engine.destroy();
  process.exit(1);
}, 3 * 60 * 1000);

engine.on('ready', () => {
  console.timeEnd('metadata');
  console.log(`\nGot ${engine.files.length} files:\n`);

  engine.files.forEach((file, idx) => {
    const sizeMB = (file.length / 1024 / 1024).toFixed(2);
    // Check if it looks like a video file
    const isVideo = /\.(mkv|mp4|avi|mov|wmv|ts|m4v)$/i.test(file.name);
    const marker = isVideo ? '📺' : '  ';
    console.log(`  ${marker} [${idx}] ${file.name} (${sizeMB} MB)`);
  });

  const videoFiles = engine.files.filter(f => /\.(mkv|mp4|avi|mov|wmv|ts|m4v)$/i.test(f.name));
  console.log(`\n${videoFiles.length} video files (episodes) found`);
  console.log('\nSUCCESS!');
  clearTimeout(timeout);
  engine.destroy();
  process.exit(0);
});

engine.on('error', (err) => {
  console.error('Error:', err);
});
