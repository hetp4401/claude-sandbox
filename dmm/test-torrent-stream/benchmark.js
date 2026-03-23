const torrentStream = require('torrent-stream');

const TRACKERS = [
  'udp://tracker.opentrackr.org:1337/announce',
  'udp://open.stealth.si:80/announce',
  'udp://tracker.openbittorrent.com:6969/announce',
  'udp://exodus.desync.com:6969/announce',
];

const TEST_CASES = [
  { hash: 'dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c', name: 'Big Buck Bunny (control)' },
  { hash: '605b8f1c0032682c4826caf68db37b361199b2de', name: 'Game of Thrones S01' },
  { hash: 'abdedc438f40419535b56d983e4d7c9294087613', name: 'Umbrella Academy S01' },
  { hash: 'c78059ca630629fd65fa8fb789bb8719ae33b59e', name: '12 Monkeys S01' },
  { hash: '6a32fa68e8d22685606e8e0e875a2259588a1d23', name: 'Fallout S01' },
  { hash: '9308486678f1dc29a41471eb29753aab43e4fdd9', name: 'Love Death and Robots S01' },
  { hash: '99774dd5d95b9e86f4f789f36af1730e14658c3e', name: 'Arifureta S02' },
  { hash: 'd91d484a971cebdf5b4cadb74899989795d4368a', name: "Don't Toy With Me Miss Nagatoro S01" },
];

const TIMEOUT_MS = 90_000; // 90s per torrent

function testOne(tc) {
  return new Promise((resolve) => {
    const tr = TRACKERS.map(t => `&tr=${encodeURIComponent(t)}`).join('');
    const magnet = `magnet:?xt=urn:btih:${tc.hash}${tr}`;
    const start = Date.now();

    const engine = torrentStream(magnet, {
      connections: 100,
      path: '/tmp/ts-bench',
    });

    const timer = setTimeout(() => {
      engine.destroy();
      resolve({ ...tc, status: 'TIMEOUT', timeMs: TIMEOUT_MS, files: 0, videoFiles: 0 });
    }, TIMEOUT_MS);

    engine.on('ready', () => {
      const timeMs = Date.now() - start;
      const videoFiles = engine.files.filter(f => /\.(mkv|mp4|avi|mov|wmv|ts|m4v|webm)$/i.test(f.name));
      const result = {
        ...tc,
        status: 'OK',
        timeMs,
        files: engine.files.length,
        videoFiles: videoFiles.length,
        sample: engine.files.slice(0, 3).map(f => f.name),
      };
      clearTimeout(timer);
      engine.destroy();
      resolve(result);
    });

    engine.on('error', () => {
      clearTimeout(timer);
      engine.destroy();
      resolve({ ...tc, status: 'ERROR', timeMs: Date.now() - start, files: 0, videoFiles: 0 });
    });
  });
}

async function main() {
  console.log('=== torrent-stream Benchmark ===\n');
  const results = [];

  for (const tc of TEST_CASES) {
    process.stdout.write(`  ${tc.name.padEnd(40)} ... `);
    const result = await testOne(tc);
    if (result.status === 'OK') {
      console.log(`OK  ${result.timeMs}ms  ${result.files} files (${result.videoFiles} video)`);
    } else {
      console.log(`${result.status}  ${result.timeMs}ms`);
    }
    results.push(result);
  }

  console.log('\n=== Summary ===');
  const ok = results.filter(r => r.status === 'OK');
  const failed = results.filter(r => r.status !== 'OK');
  console.log(`Resolved: ${ok.length}/${results.length}`);
  if (ok.length > 0) {
    const avgMs = Math.round(ok.reduce((s, r) => s + r.timeMs, 0) / ok.length);
    console.log(`Avg time: ${avgMs}ms`);
    console.log(`Fastest: ${Math.min(...ok.map(r => r.timeMs))}ms`);
    console.log(`Slowest: ${Math.max(...ok.map(r => r.timeMs))}ms`);
  }
  if (failed.length > 0) {
    console.log(`Failed: ${failed.map(r => r.name).join(', ')}`);
  }

  console.log('\n=== Raw JSON ===');
  console.log(JSON.stringify(results, null, 2));
}

main().catch(console.error);
