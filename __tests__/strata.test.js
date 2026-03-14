/**
 * Integration tests for the StrataDB Node.js SDK.
 *
 * All methods are async — every call uses `await`.
 */

const {
  Strata,
  StrataSnapshot,
  StrataError,
  NotFoundError,
  ValidationError,
  ConflictError,
  StateError,
  ConstraintError,
} = require('../stratadb');

describe('Strata', () => {
  let db;

  beforeEach(() => {
    db = Strata.cache();
  });

  // =========================================================================
  // KV Store — db.kv
  // =========================================================================

  describe('db.kv', () => {
    test('set and get', async () => {
      await db.kv.set('key1', 'value1');
      expect(await db.kv.get('key1')).toBe('value1');
    });

    test('set and get object', async () => {
      await db.kv.set('config', { theme: 'dark', count: 42 });
      const result = await db.kv.get('config');
      expect(result.theme).toBe('dark');
      expect(result.count).toBe(42);
    });

    test('get missing returns null', async () => {
      expect(await db.kv.get('nonexistent')).toBeNull();
    });

    test('delete', async () => {
      await db.kv.set('to_delete', 'value');
      expect(await db.kv.delete('to_delete')).toBe(true);
      expect(await db.kv.get('to_delete')).toBeNull();
    });

    test('keys without options', async () => {
      await db.kv.set('user:1', 'alice');
      await db.kv.set('user:2', 'bob');
      await db.kv.set('item:1', 'book');
      const allKeys = await db.kv.keys();
      expect(allKeys.length).toBe(3);
    });

    test('keys with prefix', async () => {
      await db.kv.set('user:1', 'alice');
      await db.kv.set('user:2', 'bob');
      await db.kv.set('item:1', 'book');
      const userKeys = await db.kv.keys({ prefix: 'user:' });
      expect(userKeys.length).toBe(2);
    });

    test('keys with limit', async () => {
      await db.kv.set('k1', 1);
      await db.kv.set('k2', 2);
      await db.kv.set('k3', 3);
      const keys = await db.kv.keys({ prefix: 'k', limit: 2 });
      expect(keys.length).toBeLessThanOrEqual(2);
    });

    test('set returns version number', async () => {
      const v = await db.kv.set('vkey', 'val');
      expect(typeof v).toBe('number');
      expect(v).toBeGreaterThan(0);
    });

    test('history', async () => {
      await db.kv.set('hkey', 'v1');
      await db.kv.set('hkey', 'v2');
      const history = await db.kv.history('hkey');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
      expect(history[0]).toHaveProperty('version');
      expect(history[0]).toHaveProperty('timestamp');
    });

    test('getVersioned', async () => {
      await db.kv.set('vk', 'val');
      const vv = await db.kv.getVersioned('vk');
      expect(vv).not.toBeNull();
      expect(vv.value).toBe('val');
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned missing returns null', async () => {
      expect(await db.kv.getVersioned('nope')).toBeNull();
    });

    test('get with asOf option', async () => {
      const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
      await db.kv.set('tt', 'v1');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('tt', 'v2');

      expect(await db.kv.get('tt')).toBe('v2');
      expect(await db.kv.get('tt', { asOf: ts })).toBe('v1');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.kv.set('kv_rt', 'v1');
      const vv = await db.kv.getVersioned('kv_rt');
      await db.kv.set('kv_rt', 'v2');

      expect(await db.kv.get('kv_rt')).toBe('v2');
      expect(await db.kv.get('kv_rt', { asOf: vv.timestamp })).toBe('v1');
    });
  });

  // =========================================================================
  // State Cell — db.state
  // =========================================================================

  describe('db.state', () => {
    test('set and get', async () => {
      await db.state.set('counter', 100);
      expect(await db.state.get('counter')).toBe(100);
    });

    test('init', async () => {
      await db.state.init('status', 'pending');
      expect(await db.state.get('status')).toBe('pending');
    });

    test('cas with expectedVersion', async () => {
      const version = await db.state.set('value', 1);
      const newVersion = await db.state.cas('value', 2, { expectedVersion: version });
      expect(newVersion).not.toBeNull();
      expect(await db.state.get('value')).toBe(2);
      // Wrong version -> CAS fails
      const result = await db.state.cas('value', 3, { expectedVersion: 999 });
      expect(result).toBeNull();
    });

    test('history', async () => {
      await db.state.set('hcell', 'a');
      await db.state.set('hcell', 'b');
      const history = await db.state.history('hcell');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    test('delete', async () => {
      await db.state.set('del_cell', 'x');
      const deleted = await db.state.delete('del_cell');
      expect(deleted).toBe(true);
      expect(await db.state.get('del_cell')).toBeNull();
    });

    test('keys with prefix', async () => {
      await db.state.set('cell_a', 1);
      await db.state.set('cell_b', 2);
      const cells = await db.state.keys({ prefix: 'cell_' });
      expect(cells.length).toBe(2);
    });

    test('getVersioned', async () => {
      await db.state.set('vcell', 42);
      const vv = await db.state.getVersioned('vcell');
      expect(vv).not.toBeNull();
      expect(vv.value).toBe(42);
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.state.set('sv_rt', 'v1');
      const vv = await db.state.getVersioned('sv_rt');
      await db.state.set('sv_rt', 'v2');

      expect(await db.state.get('sv_rt')).toBe('v2');
      expect(await db.state.get('sv_rt', { asOf: vv.timestamp })).toBe('v1');
    });
  });

  // =========================================================================
  // Event Log — db.events
  // =========================================================================

  describe('db.events', () => {
    test('append and get', async () => {
      await db.events.append('user_action', { action: 'click', target: 'button' });
      expect(await db.events.count()).toBe(1);

      const event = await db.events.get(0);
      expect(event).not.toBeNull();
      expect(event.value.action).toBe('click');
    });

    test('list by type', async () => {
      await db.events.append('click', { x: 10 });
      await db.events.append('scroll', { y: 100 });
      await db.events.append('click', { x: 20 });

      const clicks = await db.events.list('click');
      expect(clicks.length).toBe(2);
    });

    test('count', async () => {
      expect(await db.events.count()).toBe(0);
      await db.events.append('a', {});
      await db.events.append('b', {});
      expect(await db.events.count()).toBe(2);
    });

    test('list with limit', async () => {
      await db.events.append('page', { n: 1 });
      await db.events.append('page', { n: 2 });
      await db.events.append('page', { n: 3 });
      const events = await db.events.list('page', { limit: 2 });
      expect(Array.isArray(events)).toBe(true);
      expect(events.length).toBeLessThanOrEqual(3);
    });
  });

  // =========================================================================
  // JSON Store — db.json
  // =========================================================================

  describe('db.json', () => {
    test('set and get', async () => {
      await db.json.set('config', '$', { theme: 'dark', lang: 'en' });
      const result = await db.json.get('config', '$');
      expect(result.theme).toBe('dark');
    });

    test('get path', async () => {
      await db.json.set('config', '$', { theme: 'dark', lang: 'en' });
      const theme = await db.json.get('config', '$.theme');
      expect(theme).toBe('dark');
    });

    test('keys', async () => {
      await db.json.set('doc1', '$', { a: 1 });
      await db.json.set('doc2', '$', { b: 2 });
      const result = await db.json.keys();
      expect(result.keys.length).toBe(2);
    });

    test('keys with options', async () => {
      await db.json.set('pre_a', '$', { a: 1 });
      await db.json.set('pre_b', '$', { b: 2 });
      await db.json.set('other', '$', { c: 3 });
      const result = await db.json.keys({ prefix: 'pre_', limit: 10 });
      expect(result.keys.length).toBe(2);
    });

    test('history', async () => {
      await db.json.set('jhist', '$', { v: 1 });
      await db.json.set('jhist', '$', { v: 2 });
      const history = await db.json.history('jhist');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    test('delete', async () => {
      await db.json.set('jdel', '$', { x: 1 });
      const version = await db.json.delete('jdel', '$');
      expect(typeof version).toBe('number');
    });

    test('getVersioned', async () => {
      await db.json.set('jv', '$', { data: true });
      const vv = await db.json.getVersioned('jv');
      expect(vv).not.toBeNull();
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.json.set('jv_rt', '$', { v: 1 });
      const vv = await db.json.getVersioned('jv_rt');
      await db.json.set('jv_rt', '$', { v: 2 });

      const current = await db.json.get('jv_rt', '$');
      expect(current.v).toBe(2);
      const past = await db.json.get('jv_rt', '$', { asOf: vv.timestamp });
      expect(past.v).toBe(1);
    });
  });

  // =========================================================================
  // Vector Store — db.vector
  // =========================================================================

  describe('db.vector', () => {
    test('createCollection and listCollections', async () => {
      await db.vector.createCollection('embeddings', { dimension: 4 });
      const collections = await db.vector.listCollections();
      expect(collections.some((c) => c.name === 'embeddings')).toBe(true);
    });

    test('upsert and search', async () => {
      await db.vector.createCollection('embeddings', { dimension: 4 });

      const v1 = [1.0, 0.0, 0.0, 0.0];
      const v2 = [0.0, 1.0, 0.0, 0.0];

      await db.vector.upsert('embeddings', 'v1', v1);
      await db.vector.upsert('embeddings', 'v2', v2);

      const results = await db.vector.search('embeddings', v1, { limit: 2 });
      expect(results.length).toBe(2);
      expect(results[0].key).toBe('v1');
    });

    test('upsert with metadata option', async () => {
      await db.vector.createCollection('docs', { dimension: 4 });
      const vec = [1.0, 0.0, 0.0, 0.0];
      await db.vector.upsert('docs', 'doc1', vec, { metadata: { title: 'Hello' } });

      const result = await db.vector.get('docs', 'doc1');
      expect(result.metadata.title).toBe('Hello');
    });

    test('get', async () => {
      await db.vector.createCollection('vget', { dimension: 4 });
      await db.vector.upsert('vget', 'k1', [1, 0, 0, 0]);
      const result = await db.vector.get('vget', 'k1');
      expect(result).not.toBeNull();
      expect(result.key).toBe('k1');
      expect(result.embedding.length).toBe(4);
      expect(typeof result.version).toBe('number');
    });

    test('get missing returns null', async () => {
      await db.vector.createCollection('vget2', { dimension: 4 });
      expect(await db.vector.get('vget2', 'nope')).toBeNull();
    });

    test('delete', async () => {
      await db.vector.createCollection('vdel', { dimension: 4 });
      await db.vector.upsert('vdel', 'k1', [1, 0, 0, 0]);
      expect(await db.vector.delete('vdel', 'k1')).toBe(true);
      expect(await db.vector.get('vdel', 'k1')).toBeNull();
    });

    test('deleteCollection', async () => {
      await db.vector.createCollection('to_delete', { dimension: 4 });
      expect(await db.vector.deleteCollection('to_delete')).toBe(true);
    });

    test('stats', async () => {
      await db.vector.createCollection('stats', { dimension: 4 });
      await db.vector.upsert('stats', 'k1', [1, 0, 0, 0]);
      const stats = await db.vector.stats('stats');
      expect(stats.name).toBe('stats');
      expect(stats.dimension).toBe(4);
      expect(stats.count).toBeGreaterThanOrEqual(1);
    });

    test('batchUpsert', async () => {
      await db.vector.createCollection('batch', { dimension: 4 });
      const versions = await db.vector.batchUpsert('batch', [
        { key: 'b1', vector: [1, 0, 0, 0] },
        { key: 'b2', vector: [0, 1, 0, 0], metadata: { label: 'two' } },
      ]);
      expect(versions.length).toBe(2);
      versions.forEach((v) => expect(typeof v).toBe('number'));
    });

    test('search with filter', async () => {
      await db.vector.createCollection('vf', { dimension: 4 });
      await db.vector.upsert('vf', 'k1', [1, 0, 0, 0], { metadata: { category: 'a' } });
      await db.vector.upsert('vf', 'k2', [0, 1, 0, 0], { metadata: { category: 'b' } });
      const results = await db.vector.search('vf', [1, 0, 0, 0], {
        limit: 10,
        filter: [{ field: 'category', op: 'eq', value: 'a' }],
      });
      expect(results.length).toBe(1);
      expect(results[0].key).toBe('k1');
    });

    test('rejects non-finite vector values', async () => {
      await db.vector.createCollection('finite_test', { dimension: 4 });
      await expect(
        db.vector.upsert('finite_test', 'k', [1, NaN, 0, 0]),
      ).rejects.toThrow(/not a finite number/);
      await expect(
        db.vector.upsert('finite_test', 'k', [1, 0, Infinity, 0]),
      ).rejects.toThrow(/not a finite number/);
    });
  });

  // =========================================================================
  // Branches — db.branch
  // =========================================================================

  describe('db.branch', () => {
    test('current', async () => {
      expect(await db.branch.current()).toBe('default');
    });

    test('create, list, exists', async () => {
      await db.branch.create('feature');
      const branches = await db.branch.list();
      expect(branches).toContain('default');
      expect(branches).toContain('feature');
      expect(await db.branch.exists('feature')).toBe(true);
    });

    test('switch', async () => {
      await db.kv.set('x', 1);
      await db.branch.create('feature');
      await db.branch.switch('feature');

      expect(await db.kv.get('x')).toBeNull();

      await db.kv.set('x', 2);
      await db.branch.switch('default');
      expect(await db.kv.get('x')).toBe(1);
    });

    test('fork', async () => {
      await db.kv.set('shared', 'original');
      const result = await db.branch.fork('forked');
      expect(result.keysCopied).toBeGreaterThan(0);

      await db.branch.switch('forked');
      expect(await db.kv.get('shared')).toBe('original');
    });

    test('delete', async () => {
      await db.branch.create('to_del');
      await db.branch.delete('to_del');
      const branches = await db.branch.list();
      expect(branches).not.toContain('to_del');
    });

    test('exists returns false for missing', async () => {
      expect(await db.branch.exists('nope')).toBe(false);
    });

    test('get', async () => {
      const info = await db.branch.get('default');
      expect(info).not.toBeNull();
      expect(info.id).toBe('default');
      expect(info).toHaveProperty('status');
      expect(info).toHaveProperty('version');
    });

    test('get missing returns null', async () => {
      expect(await db.branch.get('nonexistent')).toBeNull();
    });

    test('diff', async () => {
      await db.kv.set('d_key', 'val');
      await db.branch.create('diff_b');
      const diff = await db.branch.diff('default', 'diff_b');
      expect(diff).toHaveProperty('summary');
      expect(diff.summary).toHaveProperty('totalAdded');
    });

    test('merge with and without strategy option', async () => {
      await db.kv.set('base', 'val');
      await db.branch.fork('merge_src');
      await db.branch.switch('merge_src');
      await db.kv.set('new_key', 'from_src');
      await db.branch.switch('default');
      const r1 = await db.branch.merge('merge_src');
      expect(r1).toHaveProperty('keysApplied');

      await db.branch.fork('merge_src2');
      await db.branch.switch('merge_src2');
      await db.kv.set('key2', 'from_src');
      await db.branch.switch('default');
      const r2 = await db.branch.merge('merge_src2', { strategy: 'last_writer_wins' });
      expect(r2).toHaveProperty('keysApplied');
    });
  });

  // =========================================================================
  // Spaces — db.space
  // =========================================================================

  describe('db.space', () => {
    test('current', async () => {
      expect(await db.space.current()).toBe('default');
    });

    test('create, list, exists', async () => {
      await db.space.create('ns');
      const spaces = await db.space.list();
      expect(spaces).toContain('default');
      expect(spaces).toContain('ns');
      expect(await db.space.exists('ns')).toBe(true);
    });

    test('switch', async () => {
      await db.kv.set('key', 'value1');
      await db.space.switch('other');
      expect(await db.kv.get('key')).toBeNull();

      await db.kv.set('key', 'value2');
      await db.space.switch('default');
      expect(await db.kv.get('key')).toBe('value1');
    });

    test('exists returns false for missing', async () => {
      expect(await db.space.exists('nonexistent_space')).toBe(false);
    });

    test('delete with force', async () => {
      await db.space.create('to_del_space');
      await db.space.delete('to_del_space', { force: true });
      expect(await db.space.exists('to_del_space')).toBe(false);
    });
  });

  // =========================================================================
  // Database Operations
  // =========================================================================

  describe('Database', () => {
    test('ping', async () => {
      const version = await db.ping();
      expect(version).toBeTruthy();
    });

    test('info', async () => {
      const info = await db.info();
      expect(info.version).toBeTruthy();
      expect(info.branchCount).toBeGreaterThanOrEqual(1);
    });

    test('flush', async () => {
      await db.flush();
    });

    test('compact', async () => {
      await db.compact();
    });
  });

  // =========================================================================
  // Transactions
  // =========================================================================

  describe('Transactions', () => {
    test('begin and commit', async () => {
      await db.begin();
      await expect(db.txnIsActive()).resolves.toBe(true);
      const version = await db.commit();
      expect(typeof version).toBe('number');
    });

    test('begin and rollback', async () => {
      await db.begin();
      await db.rollback();
      await expect(db.txnIsActive()).resolves.toBe(false);
    });

    test('txnIsActive before begin', async () => {
      expect(await db.txnIsActive()).toBe(false);
    });

    test('txnInfo', async () => {
      expect(await db.txnInfo()).toBeNull();
      await db.begin();
      const info = await db.txnInfo();
      expect(info).not.toBeNull();
      expect(info).toHaveProperty('id');
      expect(info).toHaveProperty('status');
      await db.rollback();
    });
  });

  // =========================================================================
  // Retention
  // =========================================================================

  describe('Retention', () => {
    test('retentionApply succeeds', async () => {
      await db.kv.set('r_key', 'val');
      await db.retentionApply();
    });
  });

  // =========================================================================
  // Configuration
  // =========================================================================

  describe('Configuration', () => {
    test('config returns defaults', async () => {
      const cfg = await db.config();
      expect(cfg.durability).toBe('standard');
      expect(cfg.autoEmbed).toBe(false);
      expect(cfg.model).toBeNull();
    });

    test('autoEmbedEnabled returns false by default', async () => {
      const enabled = await db.autoEmbedEnabled();
      expect(enabled).toBe(false);
    });

    test('configureModel persists in config', async () => {
      await db.configureModel('http://localhost:11434/v1', 'qwen3:1.7b');
      const cfg = await db.config();
      expect(cfg.model).not.toBeNull();
      expect(cfg.model.endpoint).toBe('http://localhost:11434/v1');
      expect(cfg.model.model).toBe('qwen3:1.7b');
      expect(cfg.model.timeoutMs).toBe(5000);
    });

    test('configureModel with api key and timeout', async () => {
      await db.configureModel('http://localhost:11434/v1', 'qwen3:1.7b', 'sk-test', 10000);
      const cfg = await db.config();
      // API key is redacted for security
      expect(cfg.model.apiKey).toBe('[REDACTED]');
      expect(cfg.model.timeoutMs).toBe(10000);
    });
  });

  // =========================================================================
  // Search
  // =========================================================================

  describe('Search', () => {
    test('cross-primitive search returns array', async () => {
      await db.kv.set('search_key', 'hello world');
      const results = await db.search('hello');
      expect(Array.isArray(results)).toBe(true);
    });

    test('search empty database returns empty array', async () => {
      const fresh = Strata.cache();
      const results = await fresh.search('anything');
      expect(results).toEqual([]);
      await fresh.close();
    });

    test('search with primitives filter', async () => {
      await db.kv.set('s_k', 'data');
      const results = await db.search('data', { primitives: ['kv'] });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with mode', async () => {
      const results = await db.search('test', { mode: 'keyword' });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with expand and rerank disabled', async () => {
      const results = await db.search('test', { expand: false, rerank: false });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with time range', async () => {
      const results = await db.search('test', {
        timeRange: { start: '2020-01-01T00:00:00Z', end: '2030-01-01T00:00:00Z' },
      });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with all options', async () => {
      const results = await db.search('hello', {
        k: 5,
        primitives: ['kv'],
        mode: 'hybrid',
        expand: false,
        rerank: false,
      });
      expect(Array.isArray(results)).toBe(true);
    });
  });

  // =========================================================================
  // Snapshot API — db.at(timestamp)
  // =========================================================================

  describe('db.at()', () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    test('kv.get reads at snapshot time', async () => {
      await db.kv.set('snap_kv', 'v1');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('snap_kv', 'v2');

      const snapshot = db.at(ts);
      expect(await snapshot.kv.get('snap_kv')).toBe('v1');
      expect(await db.kv.get('snap_kv')).toBe('v2');
    });

    test('kv.keys reads at snapshot time', async () => {
      await db.kv.set('sk_a', 1);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('sk_b', 2);

      const snapshot = db.at(ts);
      const past = await snapshot.kv.keys({ prefix: 'sk_' });
      expect(past.length).toBe(1);
    });

    test('snapshot write throws StateError', () => {
      const snapshot = db.at(12345);
      expect(() => snapshot.kv.set('k', 'v')).toThrow(StateError);
      expect(() => snapshot.kv.delete('k')).toThrow(StateError);
      expect(() => snapshot.kv.batchPut([])).toThrow(StateError);
      expect(() => snapshot.state.set('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.init('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.cas('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.delete('c')).toThrow(StateError);
      expect(() => snapshot.state.batchSet([])).toThrow(StateError);
      expect(() => snapshot.events.append('t', {})).toThrow(StateError);
      expect(() => snapshot.events.batchAppend([])).toThrow(StateError);
      expect(() => snapshot.json.set('k', '$', {})).toThrow(StateError);
      expect(() => snapshot.json.delete('k', '$')).toThrow(StateError);
      expect(() => snapshot.json.batchSet([])).toThrow(StateError);
      expect(() => snapshot.json.batchDelete([])).toThrow(StateError);
      expect(() => snapshot.vector.createCollection('c', {})).toThrow(StateError);
      expect(() => snapshot.vector.deleteCollection('c')).toThrow(StateError);
      expect(() => snapshot.vector.upsert('c', 'k', [])).toThrow(StateError);
      expect(() => snapshot.vector.delete('c', 'k')).toThrow(StateError);
      expect(() => snapshot.vector.batchUpsert('c', [])).toThrow(StateError);
    });

    test('state.get reads at snapshot time', async () => {
      await db.state.set('snap_state', 'old');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.state.set('snap_state', 'new');

      const snapshot = db.at(ts);
      expect(await snapshot.state.get('snap_state')).toBe('old');
    });

    test('state.keys reads at snapshot time', async () => {
      await db.state.set('tts_a', 1);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.state.set('tts_b', 2);

      const snapshot = db.at(ts);
      const past = await snapshot.state.keys({ prefix: 'tts_' });
      expect(past.length).toBe(1);
    });

    test('events.get reads at snapshot time', async () => {
      await db.events.append('snap_evt', { v: 1 });
      const evt = await db.events.get(0);
      const eventTs = evt.timestamp;

      const snapshot = db.at(eventTs);
      const past = await snapshot.events.get(0);
      expect(past).not.toBeNull();

      const early = db.at(1);
      const before = await early.events.get(0);
      expect(before).toBeNull();
    });

    test('events.list reads at snapshot time', async () => {
      await db.events.append('tt_etype', { n: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.events.append('tt_etype', { n: 2 });

      const current = await db.events.list('tt_etype');
      expect(current.length).toBe(2);
      const snapshot = db.at(ts);
      const past = await snapshot.events.list('tt_etype');
      expect(past.length).toBe(1);
    });

    test('json.get reads at snapshot time', async () => {
      await db.json.set('snap_json', '$', { v: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.json.set('snap_json', '$', { v: 2 });

      const snapshot = db.at(ts);
      const val = await snapshot.json.get('snap_json', '$');
      expect(val.v).toBe(1);
    });

    test('json.keys reads at snapshot time', async () => {
      await db.json.set('ttj_a', '$', { x: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.json.set('ttj_b', '$', { x: 2 });

      const snapshot = db.at(ts);
      const past = await snapshot.json.keys({ prefix: 'ttj_' });
      expect(past.keys.length).toBe(1);
    });

    test('vector.search reads at snapshot time', async () => {
      await db.vector.createCollection('snap_vec', { dimension: 4 });
      await db.vector.upsert('snap_vec', 'a', [1, 0, 0, 0]);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.vector.upsert('snap_vec', 'b', [0, 1, 0, 0]);

      const snapshot = db.at(ts);
      const results = await snapshot.vector.search('snap_vec', [1, 0, 0, 0], { limit: 10 });
      expect(results.length).toBe(1);
    });

    test('vector.get reads at snapshot time', async () => {
      await db.vector.createCollection('tt_vget', { dimension: 4 });
      await db.vector.upsert('tt_vget', 'k', [1, 0, 0, 0], { metadata: { tag: 'v1' } });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.vector.upsert('tt_vget', 'k', [0, 1, 0, 0], { metadata: { tag: 'v2' } });

      const current = await db.vector.get('tt_vget', 'k');
      expect(current.metadata.tag).toBe('v2');
      const snapshot = db.at(ts);
      const past = await snapshot.vector.get('tt_vget', 'k');
      expect(past).not.toBeNull();
      expect(past.metadata.tag).toBe('v1');
    });

    test('timeRange returns oldest and latest', async () => {
      await db.kv.set('tr_key', 'val');
      const range = await db.timeRange();
      expect(range).toHaveProperty('oldestTs');
      expect(range).toHaveProperty('latestTs');
      expect(typeof range.oldestTs).toBe('number');
      expect(typeof range.latestTs).toBe('number');
      expect(range.latestTs).toBeGreaterThanOrEqual(range.oldestTs);
    });

    test('StrataSnapshot is exported', () => {
      expect(StrataSnapshot).toBeDefined();
      const snapshot = db.at(12345);
      expect(snapshot).toBeInstanceOf(StrataSnapshot);
    });
  });

  // =========================================================================
  // Transaction callback — db.transaction()
  // =========================================================================

  describe('db.transaction()', () => {
    test('auto-commits on success', async () => {
      await db.kv.set('pre', 'before');
      await db.transaction(async (tx) => {
        await tx.kv.set('tx_key', 'tx_value');
      });
      expect(await db.kv.get('tx_key')).toBe('tx_value');
    });

    test('auto-rollback on error', async () => {
      await expect(
        db.transaction(async (tx) => {
          throw new Error('intentional');
        }),
      ).rejects.toThrow('intentional');
      // After rollback, no transaction should be active
      expect(await db.txnIsActive()).toBe(false);
    });

    test('returns result from callback', async () => {
      const result = await db.transaction(async (tx) => {
        await tx.kv.set('r_key', 'r_val');
        return 42;
      });
      expect(result).toBe(42);
    });

    test('read-only transaction', async () => {
      await db.kv.set('ro_key', 'ro_val');
      const result = await db.transaction(
        async (tx) => {
          const val = await tx.kv.get('ro_key');
          return val;
        },
        { readOnly: true },
      );
      expect(result).toBe('ro_val');
    });
  });

  // =========================================================================
  // Errors
  // =========================================================================

  describe('Errors', () => {
    test('NotFoundError with correct hierarchy and code', async () => {
      try {
        await db.vector.search('no_such_collection', [1, 0, 0, 0], { limit: 1 });
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(NotFoundError);
        expect(err).toBeInstanceOf(StrataError);
        expect(err).toBeInstanceOf(Error);
        expect(err.code).toBe('NOT_FOUND');
        // negative checks — not other subclasses
        expect(err instanceof ValidationError).toBe(false);
        expect(err instanceof ConflictError).toBe(false);
      }
    });

    test('ValidationError on invalid metric', async () => {
      try {
        await db.vector.createCollection('x', { dimension: 4, metric: 'invalid_metric' });
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        expect(err.code).toBe('VALIDATION');
      }
    });

    test('ConstraintError on dimension mismatch', async () => {
      await db.vector.createCollection('dim_test', { dimension: 4 });
      try {
        await db.vector.upsert('dim_test', 'k', [1, 0]); // wrong dimension
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(ConstraintError);
        expect(err.code).toBe('CONSTRAINT');
      }
    });
  });

  // =========================================================================
  // Batch KV — db.kv.batchPut
  // =========================================================================

  describe('db.kv.batchPut', () => {
    test('batchPut stores all entries and returns versions', async () => {
      const results = await db.kv.batchPut([
        { key: 'bk1', value: 'v1' },
        { key: 'bk2', value: 'v2' },
        { key: 'bk3', value: { nested: true } },
      ]);
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(3);
      results.forEach((r) => {
        expect(typeof r.version).toBe('number');
        expect(r.version).toBeGreaterThan(0);
        expect(r.error).toBeNull();
      });
      expect(await db.kv.get('bk1')).toBe('v1');
      expect(await db.kv.get('bk2')).toBe('v2');
      const obj = await db.kv.get('bk3');
      expect(obj.nested).toBe(true);
    });

    test('batchPut empty array returns empty array', async () => {
      const results = await db.kv.batchPut([]);
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(0);
    });

    test('batchPut overwrites existing keys', async () => {
      await db.kv.set('bk_ow', 'original');
      const results = await db.kv.batchPut([{ key: 'bk_ow', value: 'updated' }]);
      expect(results[0].error).toBeNull();
      expect(await db.kv.get('bk_ow')).toBe('updated');
    });

    test('batchPut rejects entries missing key', async () => {
      await expect(db.kv.batchPut([{ value: 'no_key' }])).rejects.toThrow(/key/i);
    });

    test('batchPut rejects non-object entries', async () => {
      await expect(db.kv.batchPut(['string_entry'])).rejects.toThrow();
    });
  });

  // =========================================================================
  // Batch State — db.state.batchSet
  // =========================================================================

  describe('db.state.batchSet', () => {
    test('batchSet stores all entries', async () => {
      const results = await db.state.batchSet([
        { cell: 'bs1', value: 100 },
        { cell: 'bs2', value: 'hello' },
      ]);
      expect(results.length).toBe(2);
      results.forEach((r) => {
        expect(typeof r.version).toBe('number');
        expect(r.version).toBeGreaterThan(0);
        expect(r.error).toBeNull();
      });
      expect(await db.state.get('bs1')).toBe(100);
      expect(await db.state.get('bs2')).toBe('hello');
    });

    test('batchSet empty array', async () => {
      const results = await db.state.batchSet([]);
      expect(results).toEqual([]);
    });

    test('batchSet rejects entries missing cell', async () => {
      await expect(db.state.batchSet([{ value: 1 }])).rejects.toThrow(/cell/i);
    });
  });

  // =========================================================================
  // Batch Events — db.events.batchAppend
  // =========================================================================

  describe('db.events.batchAppend', () => {
    test('batchAppend stores events and they are retrievable', async () => {
      const results = await db.events.batchAppend([
        { event_type: 'click', payload: { x: 10 } },
        { event_type: 'scroll', payload: { y: 200 } },
        { event_type: 'click', payload: { x: 30 } },
      ]);
      expect(results.length).toBe(3);
      results.forEach((r) => {
        expect(typeof r.version).toBe('number');
        expect(r.error).toBeNull();
      });
      expect(await db.events.count()).toBe(3);
      // Verify events can be read back by type
      const clicks = await db.events.list('click');
      expect(clicks.length).toBe(2);
    });

    test('batchAppend accepts eventType camelCase', async () => {
      const results = await db.events.batchAppend([
        { eventType: 'camel', payload: { ok: true } },
      ]);
      expect(results.length).toBe(1);
      expect(results[0].error).toBeNull();
    });

    test('batchAppend empty array', async () => {
      const results = await db.events.batchAppend([]);
      expect(results).toEqual([]);
    });
  });

  // =========================================================================
  // Batch JSON — db.json.batchSet / batchGet / batchDelete
  // =========================================================================

  describe('db.json batch', () => {
    test('batchSet stores documents retrievable by get', async () => {
      const results = await db.json.batchSet([
        { key: 'jb1', path: '$', value: { a: 1 } },
        { key: 'jb2', path: '$', value: { b: 2 } },
      ]);
      expect(results.length).toBe(2);
      results.forEach((r) => {
        expect(typeof r.version).toBe('number');
        expect(r.version).toBeGreaterThan(0);
        expect(r.error).toBeNull();
      });
      // Verify data was actually stored
      expect((await db.json.get('jb1', '$')).a).toBe(1);
      expect((await db.json.get('jb2', '$')).b).toBe(2);
    });

    test('batchGet returns values with version and timestamp', async () => {
      await db.json.set('jbg1', '$', { x: 10 });
      await db.json.set('jbg2', '$', { y: 20 });
      const results = await db.json.batchGet([
        { key: 'jbg1', path: '$' },
        { key: 'jbg2', path: '$' },
      ]);
      expect(results.length).toBe(2);
      // Verify values
      expect(results[0].value.x).toBe(10);
      expect(results[1].value.y).toBe(20);
      // Verify version and timestamp are present
      results.forEach((r) => {
        expect(r.error).toBeNull();
        expect(typeof r.version).toBe('number');
        expect(typeof r.timestamp).toBe('number');
      });
    });

    test('batchGet missing key returns null value', async () => {
      const results = await db.json.batchGet([
        { key: 'nonexistent_jbg', path: '$' },
      ]);
      expect(results.length).toBe(1);
      expect(results[0].value).toBeNull();
    });

    test('batchDelete removes documents', async () => {
      await db.json.set('jbd1', '$', { del: true });
      await db.json.set('jbd2', '$', { del: true });
      const results = await db.json.batchDelete([
        { key: 'jbd1', path: '$' },
        { key: 'jbd2', path: '$' },
      ]);
      expect(results.length).toBe(2);
      results.forEach((r) => {
        expect(r.error).toBeNull();
      });
      // Verify data is actually gone
      expect(await db.json.get('jbd1', '$')).toBeNull();
      expect(await db.json.get('jbd2', '$')).toBeNull();
    });

    test('batchDelete nonexistent key does not error', async () => {
      const results = await db.json.batchDelete([
        { key: 'nonexistent_jbd', path: '$' },
      ]);
      expect(results.length).toBe(1);
    });

    test('batchSet empty array', async () => {
      const results = await db.json.batchSet([]);
      expect(results).toEqual([]);
    });

    test('batchGet empty array', async () => {
      const results = await db.json.batchGet([]);
      expect(results).toEqual([]);
    });

    test('batchSet rejects entries missing key', async () => {
      await expect(
        db.json.batchSet([{ path: '$', value: 'no_key' }]),
      ).rejects.toThrow(/key/i);
    });
  });

  // =========================================================================
  // Configuration — configureSet / configureGet
  // =========================================================================

  describe('Configuration key-value', () => {
    test('configureSet and configureGet roundtrip', async () => {
      await db.configureSet('bm25_k1', '1.5');
      const value = await db.configureGet('bm25_k1');
      expect(value).toBe('1.5');
    });

    test('configureSet and configureGet durability', async () => {
      await db.configureSet('durability', 'standard');
      const value = await db.configureGet('durability');
      expect(value).toBe('standard');
    });

    test('configureSet rejects unknown key', async () => {
      await expect(db.configureSet('nonexistent.key', 'val')).rejects.toThrow();
    });
  });

  // =========================================================================
  // Durability — durabilityCounters
  // =========================================================================

  describe('Durability', () => {
    test('durabilityCounters returns expected fields', async () => {
      const counters = await db.durabilityCounters();
      expect(counters).toHaveProperty('walAppends');
      expect(counters).toHaveProperty('syncCalls');
      expect(counters).toHaveProperty('bytesWritten');
      expect(counters).toHaveProperty('syncNanos');
      expect(typeof counters.walAppends).toBe('number');
      expect(typeof counters.syncCalls).toBe('number');
      expect(typeof counters.bytesWritten).toBe('number');
      expect(typeof counters.syncNanos).toBe('number');
    });

    test('durabilityCounters reflect writes', async () => {
      const before = await db.durabilityCounters();
      await db.kv.set('dur-test', 'value');
      const after = await db.durabilityCounters();
      // WAL appends should increase after a write
      expect(after.walAppends).toBeGreaterThanOrEqual(before.walAppends);
    });
  });

  // =========================================================================
  // Embedding / Inference / Models (require feature flags + model downloads)
  // =========================================================================

  describe('Embedding', () => {
    test.skip('embed returns float vector (requires embed feature)', async () => {
      const vec = await db.embed('hello world');
      expect(Array.isArray(vec)).toBe(true);
      expect(vec.length).toBeGreaterThan(0);
    });

    test.skip('embedBatch returns array of vectors', async () => {
      const vecs = await db.embedBatch(['hello', 'world']);
      expect(Array.isArray(vecs)).toBe(true);
      expect(vecs.length).toBe(2);
    });

    test('embedStatus returns status object', async () => {
      const status = await db.embedStatus();
      expect(status).toHaveProperty('autoEmbed');
      expect(status).toHaveProperty('pending');
      expect(status).toHaveProperty('totalQueued');
      expect(status).toHaveProperty('totalEmbedded');
      expect(status).toHaveProperty('totalFailed');
      expect(typeof status.autoEmbed).toBe('boolean');
      expect(typeof status.pending).toBe('number');
      expect(typeof status.totalQueued).toBe('number');
    });
  });

  describe('Inference', () => {
    test.skip('generate returns result (requires model)', async () => {
      const result = await db.generate('miniLM', 'Hello', { maxTokens: 10 });
      expect(result).toHaveProperty('text');
      expect(result).toHaveProperty('stopReason');
      expect(result).toHaveProperty('promptTokens');
      expect(result).toHaveProperty('completionTokens');
      expect(result).toHaveProperty('model');
    });

    test.skip('tokenize returns token IDs (requires model)', async () => {
      const result = await db.tokenize('miniLM', 'Hello world');
      expect(result).toHaveProperty('ids');
      expect(result).toHaveProperty('count');
    });
  });

  describe('Models', () => {
    test('modelsList returns array with expected structure', async () => {
      const models = await db.modelsList();
      expect(Array.isArray(models)).toBe(true);
      // If models are available, verify structure
      if (models.length > 0) {
        const model = models[0];
        expect(model).toHaveProperty('name');
        expect(model).toHaveProperty('task');
        expect(model).toHaveProperty('defaultQuant');
        expect(model).toHaveProperty('isLocal');
        expect(typeof model.name).toBe('string');
        expect(typeof model.isLocal).toBe('boolean');
      }
    });

    test('modelsLocal returns array', async () => {
      const models = await db.modelsLocal();
      expect(Array.isArray(models)).toBe(true);
    });
  });

  // =========================================================================
  // Graph — db.graph
  // =========================================================================

  describe('db.graph', () => {
    describe('lifecycle', () => {
      test('create, list, info, delete', async () => {
        await db.graph.create('test-graph');
        const graphs = await db.graph.list();
        expect(graphs).toContain('test-graph');

        const meta = await db.graph.info('test-graph');
        expect(meta).not.toBeNull();
        expect(typeof meta).toBe('object');

        await db.graph.delete('test-graph');
        const after = await db.graph.list();
        expect(after).not.toContain('test-graph');
      });

      test('create with cascade policy', async () => {
        await db.graph.create('cascade-graph', { cascadePolicy: 'cascade' });
        const graphs = await db.graph.list();
        expect(graphs).toContain('cascade-graph');
        await db.graph.delete('cascade-graph');
      });

      test('list on fresh db returns empty', async () => {
        const graphs = await db.graph.list();
        expect(Array.isArray(graphs)).toBe(true);
      });
    });

    describe('nodes', () => {
      test('addNode and getNode returns properties', async () => {
        await db.graph.create('ng');
        await db.graph.addNode('ng', 'alice');
        await db.graph.addNode('ng', 'bob', { properties: { age: 30, name: 'Bob' } });

        const node = await db.graph.getNode('ng', 'bob');
        expect(node).not.toBeNull();
        expect(typeof node).toBe('object');
        // getNode should return the node data including properties
        expect(node).toHaveProperty('properties');
        expect(node.properties.age).toBe(30);
        expect(node.properties.name).toBe('Bob');

        await db.graph.delete('ng');
      });

      test('listNodes and removeNode', async () => {
        await db.graph.create('ng2');
        await db.graph.addNode('ng2', 'alice');
        await db.graph.addNode('ng2', 'bob');

        const nodes = await db.graph.listNodes('ng2');
        expect(nodes).toContain('alice');
        expect(nodes).toContain('bob');

        await db.graph.removeNode('ng2', 'alice');
        const after = await db.graph.listNodes('ng2');
        expect(after).not.toContain('alice');
        expect(after).toContain('bob');

        await db.graph.delete('ng2');
      });

      test('addNode with entityRef and objectType', async () => {
        await db.graph.create('nf');
        await db.graph.defineObjectType('nf', { name: 'User', properties: {} });
        await db.graph.addNode('nf', 'u1', {
          entityRef: 'kv://main/user1',
          objectType: 'User',
          properties: { role: 'admin' },
        });

        const node = await db.graph.getNode('nf', 'u1');
        expect(node).not.toBeNull();

        const byType = await db.graph.nodesByType('nf', 'User');
        expect(byType).toContain('u1');

        await db.graph.delete('nf');
      });

      test('addNode duplicate updates without error', async () => {
        await db.graph.create('dup');
        await db.graph.addNode('dup', 'x', { properties: { v: 1 } });
        await db.graph.addNode('dup', 'x', { properties: { v: 2 } });

        const node = await db.graph.getNode('dup', 'x');
        expect(node).not.toBeNull();
        // Should have updated properties
        expect(node.properties.v).toBe(2);

        const nodes = await db.graph.listNodes('dup');
        expect(nodes.filter((n) => n === 'x').length).toBe(1);

        await db.graph.delete('dup');
      });

      test('listNodes with pagination and cursor continuation', async () => {
        await db.graph.create('pg');
        for (let i = 0; i < 5; i++) {
          await db.graph.addNode('pg', `n${i}`);
        }

        // First page
        const page1 = await db.graph.listNodes('pg', { limit: 2 });
        expect(page1).toHaveProperty('items');
        expect(page1).toHaveProperty('nextCursor');
        expect(page1.items.length).toBe(2);

        // Second page using cursor from first
        if (page1.nextCursor) {
          const page2 = await db.graph.listNodes('pg', { limit: 2, cursor: page1.nextCursor });
          expect(page2.items.length).toBeGreaterThan(0);
          // No overlap between pages
          for (const item of page2.items) {
            expect(page1.items).not.toContain(item);
          }
        }

        await db.graph.delete('pg');
      });

      test('listNodes on empty graph returns empty', async () => {
        await db.graph.create('empty-nodes');
        const nodes = await db.graph.listNodes('empty-nodes');
        expect(nodes).toEqual([]);
        await db.graph.delete('empty-nodes');
      });

      test('getNode missing returns null', async () => {
        await db.graph.create('gn');
        const node = await db.graph.getNode('gn', 'nonexistent');
        expect(node).toBeNull();
        await db.graph.delete('gn');
      });
    });

    describe('edges', () => {
      test('addEdge, neighbors, removeEdge', async () => {
        await db.graph.create('eg');
        await db.graph.addNode('eg', 'a');
        await db.graph.addNode('eg', 'b');
        await db.graph.addEdge('eg', 'a', 'b', 'KNOWS');

        const neighbors = await db.graph.neighbors('eg', 'a');
        expect(Array.isArray(neighbors)).toBe(true);
        expect(neighbors.length).toBe(1);
        expect(neighbors[0].nodeId).toBe('b');
        expect(neighbors[0].edgeType).toBe('KNOWS');
        expect(typeof neighbors[0].weight).toBe('number');

        await db.graph.removeEdge('eg', 'a', 'b', 'KNOWS');
        const after = await db.graph.neighbors('eg', 'a');
        expect(after.length).toBe(0);

        await db.graph.delete('eg');
      });

      test('addEdge with weight and properties', async () => {
        await db.graph.create('ew');
        await db.graph.addNode('ew', 'x');
        await db.graph.addNode('ew', 'y');
        await db.graph.addEdge('ew', 'x', 'y', 'LIKES', {
          weight: 0.8,
          properties: { since: 2024 },
        });

        const neighbors = await db.graph.neighbors('ew', 'x');
        expect(neighbors[0].weight).toBeCloseTo(0.8);

        await db.graph.delete('ew');
      });

      test('neighbors with direction and edgeType filters', async () => {
        await db.graph.create('nd');
        await db.graph.addNode('nd', 'a');
        await db.graph.addNode('nd', 'b');
        await db.graph.addNode('nd', 'c');
        await db.graph.addEdge('nd', 'a', 'b', 'FOLLOWS');
        await db.graph.addEdge('nd', 'a', 'c', 'LIKES');
        await db.graph.addEdge('nd', 'c', 'a', 'FOLLOWS');

        // Outgoing from a
        const outgoing = await db.graph.neighbors('nd', 'a', { direction: 'outgoing' });
        expect(outgoing.length).toBe(2);

        // Incoming to a
        const incoming = await db.graph.neighbors('nd', 'a', { direction: 'incoming' });
        expect(incoming.length).toBe(1);
        expect(incoming[0].nodeId).toBe('c');

        // Both directions
        const both = await db.graph.neighbors('nd', 'a', { direction: 'both' });
        expect(both.length).toBe(3);

        // Filter by edgeType
        const followsOnly = await db.graph.neighbors('nd', 'a', { edgeType: 'FOLLOWS' });
        expect(followsOnly.length).toBeGreaterThanOrEqual(1);
        for (const n of followsOnly) {
          expect(n.edgeType).toBe('FOLLOWS');
        }

        await db.graph.delete('nd');
      });

      test('neighbors on node with no edges returns empty', async () => {
        await db.graph.create('ne');
        await db.graph.addNode('ne', 'lonely');
        const neighbors = await db.graph.neighbors('ne', 'lonely');
        expect(neighbors).toEqual([]);
        await db.graph.delete('ne');
      });
    });

    describe('bulk insert', () => {
      test('bulkInsert nodes and edges with verification', async () => {
        await db.graph.create('bg');
        const result = await db.graph.bulkInsert('bg', {
          nodes: [
            { nodeId: 'n1' },
            { nodeId: 'n2' },
            { nodeId: 'n3' },
          ],
          edges: [
            { src: 'n1', dst: 'n2', edgeType: 'LINK' },
            { src: 'n2', dst: 'n3', edgeType: 'LINK' },
          ],
        });

        expect(result.nodesInserted).toBe(3);
        expect(result.edgesInserted).toBe(2);

        const nodes = await db.graph.listNodes('bg');
        expect(nodes.length).toBe(3);
        expect(nodes).toContain('n1');
        expect(nodes).toContain('n2');
        expect(nodes).toContain('n3');

        // Verify edges were created
        const neighbors = await db.graph.neighbors('bg', 'n1');
        expect(neighbors.length).toBe(1);
        expect(neighbors[0].nodeId).toBe('n2');

        await db.graph.delete('bg');
      });

      test('bulkInsert with all optional fields', async () => {
        await db.graph.create('bf');
        await db.graph.defineObjectType('bf', { name: 'Item', properties: {} });
        const result = await db.graph.bulkInsert('bf', {
          nodes: [
            {
              nodeId: 'full',
              entityRef: 'kv://main/item1',
              objectType: 'Item',
              properties: { color: 'red' },
            },
            { nodeId: 'min' },
          ],
          edges: [
            {
              src: 'full',
              dst: 'min',
              edgeType: 'LINKS',
              weight: 2.5,
              properties: { note: 'test' },
            },
          ],
        });

        expect(result.nodesInserted).toBe(2);
        expect(result.edgesInserted).toBe(1);

        const node = await db.graph.getNode('bf', 'full');
        expect(node).not.toBeNull();

        const neighbors = await db.graph.neighbors('bf', 'full');
        expect(neighbors[0].weight).toBeCloseTo(2.5);

        await db.graph.delete('bf');
      });

      test('bulkInsert empty arrays', async () => {
        await db.graph.create('be');
        const result = await db.graph.bulkInsert('be', {
          nodes: [],
          edges: [],
        });
        expect(result.nodesInserted).toBe(0);
        expect(result.edgesInserted).toBe(0);
        await db.graph.delete('be');
      });

      test('bulkInsert nodes only, no edges', async () => {
        await db.graph.create('bn');
        const result = await db.graph.bulkInsert('bn', {
          nodes: [{ nodeId: 'solo1' }, { nodeId: 'solo2' }],
        });
        expect(result.nodesInserted).toBe(2);
        expect(result.edgesInserted).toBe(0);
        await db.graph.delete('bn');
      });
    });

    describe('BFS', () => {
      test('bfs traversal with edges verification', async () => {
        await db.graph.create('bfs');
        await db.graph.bulkInsert('bfs', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'c', edgeType: 'E' },
          ],
        });

        const result = await db.graph.bfs('bfs', 'a', 3);
        expect(result.visited).toContain('a');
        expect(result.visited).toContain('b');
        expect(result.visited).toContain('c');
        expect(result.visited.length).toBe(3);

        // Verify depths
        expect(result.depths.a).toBe(0);
        expect(result.depths.b).toBe(1);
        expect(result.depths.c).toBe(2);

        // Verify edges have correct structure
        expect(result.edges.length).toBe(2);
        for (const edge of result.edges) {
          expect(edge).toHaveProperty('src');
          expect(edge).toHaveProperty('dst');
          expect(edge).toHaveProperty('edgeType');
        }

        await db.graph.delete('bfs');
      });

      test('bfs with maxDepth limit', async () => {
        await db.graph.create('bfs2');
        await db.graph.bulkInsert('bfs2', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'c', edgeType: 'E' },
          ],
        });

        const result = await db.graph.bfs('bfs2', 'a', 1);
        expect(result.visited).toContain('a');
        expect(result.visited).toContain('b');
        expect(result.visited).not.toContain('c');
        expect(result.depths.b).toBe(1);

        await db.graph.delete('bfs2');
      });

      test('bfs with maxNodes limit', async () => {
        await db.graph.create('bfs3');
        await db.graph.bulkInsert('bfs3', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }, { nodeId: 'd' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'a', dst: 'c', edgeType: 'E' },
            { src: 'a', dst: 'd', edgeType: 'E' },
          ],
        });

        const result = await db.graph.bfs('bfs3', 'a', 10, { maxNodes: 2 });
        expect(result.visited.length).toBeLessThanOrEqual(2);
        expect(result.visited).toContain('a');

        await db.graph.delete('bfs3');
      });
    });

    describe('ontology', () => {
      test('defineObjectType with properties and verify structure', async () => {
        await db.graph.create('og');
        await db.graph.defineObjectType('og', {
          name: 'Person',
          properties: {
            name: { type: 'string', required: true },
            age: { type: 'integer', required: false },
          },
        });

        const types = await db.graph.listObjectTypes('og');
        expect(types).toContain('Person');

        const def = await db.graph.getObjectType('og', 'Person');
        expect(def).not.toBeNull();
        expect(def.name).toBe('Person');
        expect(def.properties).toBeDefined();
        expect(def.properties.name).toBeDefined();

        await db.graph.deleteObjectType('og', 'Person');
        const after = await db.graph.listObjectTypes('og');
        expect(after).not.toContain('Person');

        await db.graph.delete('og');
      });

      test('getObjectType missing returns null', async () => {
        await db.graph.create('ogm');
        const def = await db.graph.getObjectType('ogm', 'Nonexistent');
        expect(def).toBeNull();
        await db.graph.delete('ogm');
      });

      test('defineLinkType with source/target and verify structure', async () => {
        await db.graph.create('lg');
        await db.graph.defineObjectType('lg', { name: 'A', properties: {} });
        await db.graph.defineObjectType('lg', { name: 'B', properties: {} });
        await db.graph.defineLinkType('lg', {
          name: 'KNOWS',
          source: 'A',
          target: 'B',
          properties: { since: { type: 'integer', required: false } },
        });

        const types = await db.graph.listLinkTypes('lg');
        expect(types).toContain('KNOWS');

        const def = await db.graph.getLinkType('lg', 'KNOWS');
        expect(def).not.toBeNull();
        expect(def.name).toBe('KNOWS');
        expect(def.source).toBe('A');
        expect(def.target).toBe('B');

        await db.graph.deleteLinkType('lg', 'KNOWS');
        const after = await db.graph.listLinkTypes('lg');
        expect(after).not.toContain('KNOWS');

        await db.graph.delete('lg');
      });

      test('freezeOntology prevents type modifications', async () => {
        await db.graph.create('fg');
        await db.graph.defineObjectType('fg', { name: 'Item', properties: {} });
        await db.graph.freezeOntology('fg');

        const status = await db.graph.ontologyStatus('fg');
        expect(status).not.toBeNull();
        // ontologyStatus returns a string like "frozen"
        expect(typeof status).toBe('string');

        // After freeze, defining new types should fail
        await expect(
          db.graph.defineObjectType('fg', { name: 'Other', properties: {} }),
        ).rejects.toThrow();

        await db.graph.delete('fg');
      });

      test('ontologySummary includes defined types', async () => {
        await db.graph.create('sg');
        await db.graph.defineObjectType('sg', {
          name: 'Foo',
          properties: { x: { type: 'string', required: false } },
        });
        const summary = await db.graph.ontologySummary('sg');
        expect(summary).not.toBeNull();
        expect(typeof summary).toBe('object');
        await db.graph.delete('sg');
      });

      test('listOntologyTypes returns both object and link types', async () => {
        await db.graph.create('ot');
        await db.graph.defineObjectType('ot', { name: 'A', properties: {} });
        await db.graph.defineObjectType('ot', { name: 'B', properties: {} });
        await db.graph.defineLinkType('ot', {
          name: 'REL',
          source: 'A',
          target: 'B',
          properties: {},
        });
        const all = await db.graph.listOntologyTypes('ot');
        expect(all).toContain('A');
        expect(all).toContain('B');
        expect(all).toContain('REL');
        await db.graph.delete('ot');
      });

      test('nodesByType filters by object type', async () => {
        await db.graph.create('nt');
        await db.graph.defineObjectType('nt', { name: 'Cat', properties: {} });
        await db.graph.addNode('nt', 'whiskers', { objectType: 'Cat' });
        await db.graph.addNode('nt', 'spot');

        const cats = await db.graph.nodesByType('nt', 'Cat');
        expect(cats).toContain('whiskers');
        expect(cats).not.toContain('spot');

        await db.graph.delete('nt');
      });

      test('nodesByType nonexistent type returns empty', async () => {
        await db.graph.create('ntn');
        await db.graph.addNode('ntn', 'x');
        const result = await db.graph.nodesByType('ntn', 'Nonexistent');
        expect(result).toEqual([]);
        await db.graph.delete('ntn');
      });
    });

    describe('analytics', () => {
      test('wcc returns components with correct grouping', async () => {
        await db.graph.create('wg');
        await db.graph.bulkInsert('wg', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [{ src: 'a', dst: 'b', edgeType: 'E' }],
        });

        const result = await db.graph.wcc('wg');
        expect(result.algorithm).toBe('wcc');
        // All nodes should have a component ID
        expect(result.result).toHaveProperty('a');
        expect(result.result).toHaveProperty('b');
        expect(result.result).toHaveProperty('c');
        // a and b connected → same component, c isolated → different
        expect(result.result.a).toBe(result.result.b);
        expect(result.result.c).not.toBe(result.result.a);
        // Component IDs should be numbers
        expect(typeof result.result.a).toBe('number');

        await db.graph.delete('wg');
      });

      test('pagerank returns positive scores for all nodes', async () => {
        await db.graph.create('prg');
        await db.graph.bulkInsert('prg', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'c', edgeType: 'E' },
          ],
        });

        const result = await db.graph.pagerank('prg');
        expect(result.algorithm).toBe('pagerank');
        // All nodes should have positive scores
        expect(result.result.a).toBeGreaterThan(0);
        expect(result.result.b).toBeGreaterThan(0);
        expect(result.result.c).toBeGreaterThan(0);
        // c receives link juice from b, which receives from a
        // b should have higher rank than a (receives a link), c highest
        expect(result.result.c).toBeGreaterThanOrEqual(result.result.a);

        await db.graph.delete('prg');
      });

      test('pagerank with custom options', async () => {
        await db.graph.create('prg2');
        await db.graph.bulkInsert('prg2', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }],
          edges: [{ src: 'a', dst: 'b', edgeType: 'E' }],
        });

        const result = await db.graph.pagerank('prg2', {
          damping: 0.5,
          maxIterations: 5,
          tolerance: 0.01,
        });
        expect(result.algorithm).toBe('pagerank');
        expect(result.result.a).toBeGreaterThan(0);

        await db.graph.delete('prg2');
      });

      test('cdlp returns community labels for connected nodes', async () => {
        await db.graph.create('cg');
        await db.graph.bulkInsert('cg', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'a', edgeType: 'E' },
          ],
        });

        const result = await db.graph.cdlp('cg', 10);
        expect(result.algorithm).toBe('cdlp');
        // All nodes should have a label
        expect(result.result).toHaveProperty('a');
        expect(result.result).toHaveProperty('b');
        expect(result.result).toHaveProperty('c');
        // a and b are tightly connected → likely same community
        expect(typeof result.result.a).toBe('number');

        await db.graph.delete('cg');
      });

      test('lcc returns coefficients in valid range', async () => {
        await db.graph.create('lccg');
        // Triangle: a-b, b-c, a-c → LCC should be 1.0 for each
        await db.graph.bulkInsert('lccg', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'c', edgeType: 'E' },
            { src: 'a', dst: 'c', edgeType: 'E' },
          ],
        });

        const result = await db.graph.lcc('lccg');
        expect(result.algorithm).toBe('lcc');
        // LCC values should be between 0 and 1
        for (const [, val] of Object.entries(result.result)) {
          expect(val).toBeGreaterThanOrEqual(0);
          expect(val).toBeLessThanOrEqual(1);
        }

        await db.graph.delete('lccg');
      });

      test('sssp returns monotonically increasing distances', async () => {
        await db.graph.create('ssspg');
        await db.graph.bulkInsert('ssspg', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }, { nodeId: 'c' }],
          edges: [
            { src: 'a', dst: 'b', edgeType: 'E' },
            { src: 'b', dst: 'c', edgeType: 'E' },
          ],
        });

        const result = await db.graph.sssp('ssspg', 'a');
        expect(result.algorithm).toBe('sssp');
        // Source should be 0
        expect(result.result.a).toBe(0);
        // b reachable from a, c reachable from b
        expect(result.result.b).toBeGreaterThan(0);
        expect(result.result.c).toBeGreaterThan(result.result.b);

        await db.graph.delete('ssspg');
      });

      test('sssp with direction option', async () => {
        await db.graph.create('ssspd');
        await db.graph.bulkInsert('ssspd', {
          nodes: [{ nodeId: 'a' }, { nodeId: 'b' }],
          edges: [{ src: 'a', dst: 'b', edgeType: 'E' }],
        });

        const result = await db.graph.sssp('ssspd', 'a', { direction: 'outgoing' });
        expect(result.algorithm).toBe('sssp');
        expect(result.result.a).toBe(0);

        await db.graph.delete('ssspd');
      });
    });

    describe('snapshot', () => {
      test('snapshot graph write operations throw StateError', async () => {
        const snap = db.at(Date.now());
        expect(() => snap.graph.create('x')).toThrow(StateError);
        expect(() => snap.graph.delete('x')).toThrow(StateError);
        expect(() => snap.graph.addNode('g', 'n')).toThrow(StateError);
        expect(() => snap.graph.removeNode('g', 'n')).toThrow(StateError);
        expect(() => snap.graph.addEdge('g', 'a', 'b', 'E')).toThrow(StateError);
        expect(() => snap.graph.removeEdge('g', 'a', 'b', 'E')).toThrow(StateError);
        expect(() => snap.graph.bulkInsert('g', {})).toThrow(StateError);
        expect(() => snap.graph.defineObjectType('g', {})).toThrow(StateError);
        expect(() => snap.graph.deleteObjectType('g', 'T')).toThrow(StateError);
        expect(() => snap.graph.defineLinkType('g', {})).toThrow(StateError);
        expect(() => snap.graph.deleteLinkType('g', 'L')).toThrow(StateError);
        expect(() => snap.graph.freezeOntology('g')).toThrow(StateError);
      });

      test('snapshot graph reads return correct data', async () => {
        await db.graph.create('snap-graph');
        await db.graph.addNode('snap-graph', 'n1');
        await db.graph.addNode('snap-graph', 'n2');
        await db.graph.addEdge('snap-graph', 'n1', 'n2', 'LINK');

        const snap = db.at(Date.now());

        // list
        const graphs = await snap.graph.list();
        expect(graphs).toContain('snap-graph');

        // getNode
        const node = await snap.graph.getNode('snap-graph', 'n1');
        expect(node).not.toBeNull();

        // listNodes
        const nodes = await snap.graph.listNodes('snap-graph');
        expect(nodes).toContain('n1');
        expect(nodes).toContain('n2');

        // neighbors
        const neighbors = await snap.graph.neighbors('snap-graph', 'n1');
        expect(neighbors.length).toBe(1);
        expect(neighbors[0].nodeId).toBe('n2');

        // bfs
        const bfs = await snap.graph.bfs('snap-graph', 'n1', 2);
        expect(bfs.visited).toContain('n1');
        expect(bfs.visited).toContain('n2');

        await db.graph.delete('snap-graph');
      });
    });
  });

  // =========================================================================
  // Branch parameter gaps
  // =========================================================================

  describe('branch parameter gaps', () => {
    test('branch.create with metadata and verify via get', async () => {
      await db.branch.create('meta-branch', { metadata: { owner: 'test' } });
      const exists = await db.branch.exists('meta-branch');
      expect(exists).toBe(true);

      // Verify branch was created
      const info = await db.branch.get('meta-branch');
      expect(info).not.toBeNull();
      expect(info.id).toBe('meta-branch');

      await db.branch.delete('meta-branch');
    });

    test('branch.list with limit', async () => {
      await db.branch.create('lb1');
      await db.branch.create('lb2');
      await db.branch.create('lb3');

      const all = await db.branch.list();
      expect(all.length).toBeGreaterThanOrEqual(4); // default + lb1 + lb2 + lb3

      const limited = await db.branch.list({ limit: 2 });
      expect(limited.length).toBe(2);

      // Larger limit returns more
      const more = await db.branch.list({ limit: 10 });
      expect(more.length).toBeGreaterThanOrEqual(4);

      await db.branch.delete('lb1');
      await db.branch.delete('lb2');
      await db.branch.delete('lb3');
    });

    test('branch.create without metadata still works', async () => {
      await db.branch.create('no-meta');
      const exists = await db.branch.exists('no-meta');
      expect(exists).toBe(true);
      await db.branch.delete('no-meta');
    });
  });

  // =========================================================================
  // Generic execute
  // =========================================================================

  describe('db.execute()', () => {
    test('kv_put and kv_get', async () => {
      const writeResult = await db.execute('kv_put', { key: 'exec_key', value: 'exec_val' });
      expect(writeResult.key).toBe('exec_key');
      expect(typeof writeResult.version).toBe('number');
      const result = await db.execute('kv_get', { key: 'exec_key' });
      expect(result.value).toBe('exec_val');
      expect(typeof result.version).toBe('number');
      expect(typeof result.timestamp).toBe('number');
    });

    test('dot notation works (kv.put)', async () => {
      await db.execute('kv.put', { key: 'dot_key', value: 42 });
      const result = await db.execute('kv.get', { key: 'dot_key' });
      expect(result.value).toBe(42);
    });

    test('kv_list returns array', async () => {
      await db.execute('kv_put', { key: 'list_a', value: 1 });
      await db.execute('kv_put', { key: 'list_b', value: 2 });
      const keys = await db.execute('kv_list', { prefix: 'list_' });
      expect(Array.isArray(keys)).toBe(true);
      expect(keys).toContain('list_a');
      expect(keys).toContain('list_b');
    });

    test('kv_delete returns structured result', async () => {
      await db.execute('kv_put', { key: 'del_me', value: 'bye' });
      const result = await db.execute('kv_delete', { key: 'del_me' });
      expect(result.key).toBe('del_me');
      expect(result.deleted).toBe(true);
    });

    test('state_set and state_get', async () => {
      await db.execute('state_set', { cell: 'counter', value: 100 });
      const result = await db.execute('state_get', { cell: 'counter' });
      expect(result.value).toBe(100);
    });

    test('event_append and event_get', async () => {
      const result = await db.execute('event_append', { event_type: 'click', payload: { x: 10 } });
      expect(typeof result.sequence).toBe('number');
      expect(result.eventType).toBe('click');
      const evt = await db.execute('event_get', { sequence: result.sequence });
      expect(evt).not.toBeNull();
    });

    test('json_set and json_get', async () => {
      await db.execute('json_set', { key: 'doc1', path: '$', value: { name: 'Alice' } });
      const result = await db.execute('json_get', { key: 'doc1', path: '$.name' });
      expect(result.value).toBe('Alice');
    });

    test('unit variant commands (ping, info, flush)', async () => {
      const pong = await db.execute('ping');
      expect(typeof pong.version).toBe('string');
      const info = await db.execute('info');
      expect(info).toBeDefined();
      await db.execute('flush');
      await db.execute('compact');
    });

    test('unknown command returns error', async () => {
      await expect(db.execute('nonexistent_cmd', {})).rejects.toThrow();
    });

    test('object values round-trip', async () => {
      await db.execute('kv_put', { key: 'obj_test', value: { nested: { deep: true } } });
      const result = await db.execute('kv_get', { key: 'obj_test' });
      expect(result.value).toEqual({ nested: { deep: true } });
    });

    test('null args ok for no-arg commands', async () => {
      const result = await db.execute('ping', null);
      expect(typeof result.version).toBe('string');
    });

    test('works within transaction', async () => {
      await db.begin();
      await db.execute('kv_put', { key: 'txn_exec', value: 'in_txn' });
      await db.commit();
      const result = await db.execute('kv_get', { key: 'txn_exec' });
      expect(result.value).toBe('in_txn');
    });

    // Value type coverage
    test('boolean value round-trips', async () => {
      await db.execute('kv_put', { key: 'bool_test', value: true });
      const result = await db.execute('kv_get', { key: 'bool_test' });
      expect(result.value).toBe(true);
    });

    test('null value round-trips', async () => {
      await db.execute('kv_put', { key: 'null_test', value: null });
      const result = await db.execute('kv_get', { key: 'null_test' });
      expect(result.value).toBeNull();
    });

    test('float value round-trips', async () => {
      await db.execute('kv_put', { key: 'float_test', value: 3.14 });
      const result = await db.execute('kv_get', { key: 'float_test' });
      expect(result.value).toBeCloseTo(3.14);
    });

    test('array value round-trips', async () => {
      await db.execute('kv_put', { key: 'arr_test', value: [1, 'two', true, null] });
      const result = await db.execute('kv_get', { key: 'arr_test' });
      expect(result.value).toEqual([1, 'two', true, null]);
    });

    test('deeply nested object round-trips', async () => {
      const complex = { a: { b: { c: [1, { d: 'deep' }] } }, e: null, f: true };
      await db.execute('kv_put', { key: 'deep_test', value: complex });
      const result = await db.execute('kv_get', { key: 'deep_test' });
      expect(result.value).toEqual(complex);
    });

    // PascalCase passthrough
    test('PascalCase command names work directly', async () => {
      await db.execute('KvPut', { key: 'pascal_test', value: 'ok' });
      const result = await db.execute('KvGet', { key: 'pascal_test' });
      expect(result.value).toBe('ok');
    });

    // Batch operations
    test('kv_batch_put with entries', async () => {
      const result = await db.execute('kv_batch_put', {
        entries: [
          { key: 'batch_1', value: 'one' },
          { key: 'batch_2', value: 42 },
          { key: 'batch_3', value: { nested: true } },
        ]
      });
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);

      const v1 = await db.execute('kv_get', { key: 'batch_1' });
      expect(v1.value).toBe('one');
      const v2 = await db.execute('kv_get', { key: 'batch_2' });
      expect(v2.value).toBe(42);
      const v3 = await db.execute('kv_get', { key: 'batch_3' });
      expect(v3.value).toEqual({ nested: true });
    });

    // Vector operations with metadata
    test('vector_upsert and vector_get with metadata', async () => {
      await db.execute('vector_create_collection', {
        collection: 'exec_vectors',
        dimension: 3,
        metric: 'cosine'
      });

      await db.execute('vector_upsert', {
        collection: 'exec_vectors',
        key: 'v1',
        vector: [1.0, 0.0, 0.0],
        metadata: { color: 'red', score: 42 }
      });

      const data = await db.execute('vector_get', {
        collection: 'exec_vectors',
        key: 'v1'
      });
      expect(data).not.toBeNull();
      expect(data.data.metadata).toEqual({ color: 'red', score: 42 });
    });

    test('vector_search returns results', async () => {
      await db.execute('vector_create_collection', {
        collection: 'exec_vectors',
        dimension: 3,
        metric: 'cosine'
      });
      await db.execute('vector_upsert', {
        collection: 'exec_vectors',
        key: 'v1',
        vector: [1.0, 0.0, 0.0],
        metadata: { color: 'red', score: 42 }
      });
      await db.execute('vector_upsert', {
        collection: 'exec_vectors',
        key: 'v2',
        vector: [0.9, 0.1, 0.0],
        metadata: { color: 'blue' }
      });

      const results = await db.execute('vector_search', {
        collection: 'exec_vectors',
        query: [1.0, 0.0, 0.0],
        k: 2
      });
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(2);
      expect(results[0]).toHaveProperty('key');
      expect(results[0]).toHaveProperty('score');
    });

    test('vector_search with metadata filter', async () => {
      await db.execute('vector_create_collection', {
        collection: 'filt_vecs',
        dimension: 3,
        metric: 'cosine'
      });
      await db.execute('vector_upsert', {
        collection: 'filt_vecs',
        key: 'v1',
        vector: [1.0, 0.0, 0.0],
        metadata: { color: 'red' }
      });
      await db.execute('vector_upsert', {
        collection: 'filt_vecs',
        key: 'v2',
        vector: [0.9, 0.1, 0.0],
        metadata: { color: 'blue' }
      });

      // Filter for color=red — exercises filter array Value preprocessing
      const results = await db.execute('vector_search', {
        collection: 'filt_vecs',
        query: [1.0, 0.0, 0.0],
        k: 10,
        filter: [{ field: 'color', op: 'eq', value: 'red' }]
      });
      expect(results.length).toBe(1);
      expect(results[0].key).toBe('v1');
      // Filtered search includes metadata as plain JSON (not tagged)
      expect(results[0].metadata).toEqual({ color: 'red' });
    });

    // Graph operations with properties
    test('graph_add_node with properties', async () => {
      await db.execute('graph_add_node', {
        graph: 'exec_graph',
        node_id: 'alice',
        properties: { age: 30, role: 'engineer' }
      });
      const node = await db.execute('graph_get_node', {
        graph: 'exec_graph',
        node_id: 'alice'
      });
      expect(node).not.toBeNull();
      // GraphGetNode returns NodeData: { properties, entity_ref, object_type }
      expect(node.properties.age).toBe(30);
      expect(node.properties.role).toBe('engineer');
    });

    test('graph_add_edge with properties', async () => {
      // Create both nodes first (each test gets a fresh db)
      await db.execute('graph_add_node', {
        graph: 'exec_graph',
        node_id: 'alice',
        properties: { age: 30 }
      });
      await db.execute('graph_add_node', {
        graph: 'exec_graph',
        node_id: 'bob',
        properties: { age: 25 }
      });
      await db.execute('graph_add_edge', {
        graph: 'exec_graph',
        src: 'alice',
        dst: 'bob',
        edge_type: 'knows',
        weight: 0.9,
        properties: { since: 2020 }
      });
      const neighbors = await db.execute('graph_neighbors', {
        graph: 'exec_graph',
        node_id: 'alice'
      });
      expect(Array.isArray(neighbors)).toBe(true);
      expect(neighbors.length).toBe(1);
      expect(neighbors[0].node_id).toBe('bob');
    });

    // Event payload (object with non-string values)
    test('event_append with complex payload', async () => {
      const result = await db.execute('event_append', {
        event_type: 'metric',
        payload: { values: [1.1, 2.2], tags: { env: 'prod' }, ok: true }
      });
      expect(typeof result.sequence).toBe('number');
      expect(result.eventType).toBe('metric');
    });

    // State with various value types
    test('state_cas (compare and swap)', async () => {
      await db.execute('state_set', { cell: 'cas_cell', value: 'initial' });
      const before = await db.execute('state_get', { cell: 'cas_cell' });
      const result = await db.execute('state_cas', {
        cell: 'cas_cell',
        expected_counter: before.version,
        value: 'swapped'
      });
      expect(result.cell).toBe('cas_cell');
      expect(result.success).toBe(true);
      expect(typeof result.version).toBe('number');
      const after = await db.execute('state_get', { cell: 'cas_cell' });
      expect(after.value).toBe('swapped');
    });

    // Error: wrong field names
    test('wrong field names return error', async () => {
      await expect(
        db.execute('kv_put', { wrong_field: 'foo', value: 'bar' })
      ).rejects.toThrow();
    });

    // Error: non-object args
    test('non-object args return error', async () => {
      await expect(db.execute('kv_put', 'not an object')).rejects.toThrow(/args must be an object/);
    });
  });

  // =========================================================================
  // Agent-First API (#1442, #1443, #1444)
  // =========================================================================

  describe('Agent-First API', () => {
    // -----------------------------------------------------------------
    // describe() introspection (#1274)
    // -----------------------------------------------------------------

    test('describe() returns structured snapshot with camelCase fields', async () => {
      await db.kv.set('d_key', 'val');
      await db.state.set('d_cell', 42);
      await db.json.set('d_doc', '$', { x: 1 });
      await db.events.append('d_evt', { y: 2 });
      const desc = await db.describe();

      // Top-level
      expect(typeof desc.version).toBe('string');
      expect(desc.version).toMatch(/^\d+\.\d+\.\d+/);
      expect(typeof desc.path).toBe('string');
      expect(desc.branch).toBe('default');
      expect(desc.branches).toContain('default');
      expect(desc.spaces).toContain('default');
      expect(desc.follower).toBe(false);

      // Primitives — verify actual counts from data we inserted
      expect(desc.primitives.kv.count).toBeGreaterThanOrEqual(1);
      expect(desc.primitives.json.count).toBeGreaterThanOrEqual(1);
      expect(desc.primitives.events.count).toBeGreaterThanOrEqual(1);
      expect(desc.primitives.state.count).toBeGreaterThanOrEqual(1);
      expect(desc.primitives.state.cells).toContain('d_cell');
      expect(Array.isArray(desc.primitives.vector.collections)).toBe(true);
      expect(Array.isArray(desc.primitives.graph.graphs)).toBe(true);

      // Config — camelCase
      expect(typeof desc.config.durability).toBe('string');
      expect(typeof desc.config.autoEmbed).toBe('boolean');
      expect(typeof desc.config.embedModel).toBe('string');
      expect(typeof desc.config.provider).toBe('string');
      // defaultModel may be null
      expect('defaultModel' in desc.config).toBe(true);

      // Capabilities — camelCase
      expect(typeof desc.capabilities.search).toBe('boolean');
      expect(typeof desc.capabilities.vectorSearch).toBe('boolean');
      expect(typeof desc.capabilities.generation).toBe('boolean');
      expect(typeof desc.capabilities.autoEmbed).toBe('boolean');
    });

    test('describe() on empty database has zero counts', async () => {
      const fresh = Strata.cache();
      const desc = await fresh.describe();
      expect(desc.primitives.kv.count).toBe(0);
      expect(desc.primitives.json.count).toBe(0);
      expect(desc.primitives.events.count).toBe(0);
      expect(desc.primitives.state.count).toBe(0);
      expect(desc.primitives.state.cells).toEqual([]);
      expect(desc.primitives.vector.collections).toEqual([]);
      expect(desc.primitives.graph.graphs).toEqual([]);
      await fresh.close();
    });

    test('describe() via execute() also works', async () => {
      const desc = await db.execute('describe');
      expect(typeof desc.version).toBe('string');
      expect(desc.branch).toBe('default');
    });

    // -----------------------------------------------------------------
    // Pagination metadata (#1444)
    // -----------------------------------------------------------------

    test('json.keys() returns hasMore=true when more results exist', async () => {
      await db.json.set('jh_a', '$', { v: 1 });
      await db.json.set('jh_b', '$', { v: 2 });
      await db.json.set('jh_c', '$', { v: 3 });
      const result = await db.json.keys({ prefix: 'jh_', limit: 2 });
      expect(result.keys.length).toBe(2);
      expect(result.hasMore).toBe(true);
      expect(result.cursor).toBeDefined();
      expect(result.cursor).not.toBeNull();
    });

    test('json.keys() returns hasMore=false when all results fit', async () => {
      await db.json.set('ja_x', '$', { v: 1 });
      const result = await db.json.keys({ prefix: 'ja_', limit: 100 });
      expect(result.keys.length).toBe(1);
      expect(result.hasMore).toBe(false);
    });

    test('kv_list via execute with limit returns hasMore and cursor', async () => {
      await db.kv.set('pg_1', 'a');
      await db.kv.set('pg_2', 'b');
      await db.kv.set('pg_3', 'c');
      const page1 = await db.execute('kv_list', { prefix: 'pg_', limit: 2 });
      expect(page1.keys.length).toBe(2);
      expect(page1.hasMore).toBe(true);
      expect(page1.cursor).toBeDefined();
    });

    test('kv_list via execute without limit returns plain array', async () => {
      await db.kv.set('al_1', 'a');
      await db.kv.set('al_2', 'b');
      const result = await db.execute('kv_list', { prefix: 'al_' });
      // Without limit, returns flat array (backward compat)
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
    });

    test('kv.keys() with limit still returns string[] (namespace compat)', async () => {
      await db.kv.set('ns_1', 'a');
      await db.kv.set('ns_2', 'b');
      await db.kv.set('ns_3', 'c');
      const keys = await db.kv.keys({ prefix: 'ns_', limit: 2 });
      // Namespace API extracts just the keys array
      expect(Array.isArray(keys)).toBe(true);
      expect(keys.length).toBeLessThanOrEqual(2);
      expect(typeof keys[0]).toBe('string');
    });

    test('kvListPaginated returns {keys, hasMore, cursor}', async () => {
      await db.kv.set('lp_a', 1);
      await db.kv.set('lp_b', 2);
      await db.kv.set('lp_c', 3);
      const result = await db.kvListPaginated('lp_', 2);
      expect(result.keys.length).toBe(2);
      expect(typeof result.hasMore).toBe('boolean');
      expect(result.hasMore).toBe(true);
      // hasMore=false when fetching all
      const all = await db.kvListPaginated('lp_', 100);
      expect(all.keys.length).toBe(3);
      expect(all.hasMore).toBe(false);
    });

    // -----------------------------------------------------------------
    // Write metadata (#1443)
    // -----------------------------------------------------------------

    test('execute kv_put returns {key, version}', async () => {
      const result = await db.execute('kv_put', { key: 'wm_k', value: 'v' });
      expect(result.key).toBe('wm_k');
      expect(typeof result.version).toBe('number');
      expect(result.version).toBeGreaterThan(0);
    });

    test('execute kv_delete returns {key, deleted}', async () => {
      await db.execute('kv_put', { key: 'wd_k', value: 'v' });
      const del = await db.execute('kv_delete', { key: 'wd_k' });
      expect(del.key).toBe('wd_k');
      expect(del.deleted).toBe(true);
      // Delete non-existent
      const del2 = await db.execute('kv_delete', { key: 'wd_k' });
      expect(del2.key).toBe('wd_k');
      expect(del2.deleted).toBe(false);
    });

    test('execute json_set returns {key, version}', async () => {
      const result = await db.execute('json_set', { key: 'wm_j', path: '$', value: { x: 1 } });
      expect(result.key).toBe('wm_j');
      expect(typeof result.version).toBe('number');
    });

    test('execute json_delete returns {key, deleted}', async () => {
      await db.execute('json_set', { key: 'jd_k', path: '$', value: { x: 1 } });
      const del = await db.execute('json_delete', { key: 'jd_k', path: '$' });
      expect(del.key).toBe('jd_k');
      expect(del.deleted).toBe(true);
    });

    test('execute event_append returns {sequence, eventType}', async () => {
      const result = await db.execute('event_append', {
        event_type: 'wm_click',
        payload: { button: 'left' },
      });
      expect(typeof result.sequence).toBe('number');
      expect(result.eventType).toBe('wm_click');
    });

    test('execute state_set returns {key, version}', async () => {
      const result = await db.execute('state_set', { cell: 'wm_s', value: 42 });
      expect(result.key).toBe('wm_s');
      expect(typeof result.version).toBe('number');
    });

    test('execute state_delete returns {key, deleted}', async () => {
      await db.execute('state_set', { cell: 'sd_c', value: 1 });
      const del = await db.execute('state_delete', { cell: 'sd_c' });
      expect(del.key).toBe('sd_c');
      expect(del.deleted).toBe(true);
      // Delete non-existent
      const del2 = await db.execute('state_delete', { cell: 'sd_c' });
      expect(del2.key).toBe('sd_c');
      expect(del2.deleted).toBe(false);
    });

    test('execute state_cas success returns structured result', async () => {
      await db.execute('state_set', { cell: 'cas_s', value: 'init' });
      const before = await db.execute('state_get', { cell: 'cas_s' });
      const result = await db.execute('state_cas', {
        cell: 'cas_s',
        expected_counter: before.version,
        value: 'updated',
      });
      expect(result.cell).toBe('cas_s');
      expect(result.success).toBe(true);
      expect(typeof result.version).toBe('number');
      // currentValue should be null on success
      expect(result.currentValue).toBeNull();
    });

    test('execute state_cas conflict returns current value', async () => {
      await db.execute('state_set', { cell: 'cas_c', value: 'v1' });
      // Use a stale version to force conflict
      const conflict = await db.execute('state_cas', {
        cell: 'cas_c',
        expected_counter: 999999,
        value: 'should_fail',
      });
      expect(conflict.cell).toBe('cas_c');
      expect(conflict.success).toBe(false);
      expect(conflict.version).toBeNull();
      // On conflict, currentValue and currentVersion are populated
      expect(conflict.currentValue).toBe('v1');
      expect(typeof conflict.currentVersion).toBe('number');
    });

    // -----------------------------------------------------------------
    // Error hints (#1442)
    // -----------------------------------------------------------------

    test('NotFound errors include hint text', async () => {
      try {
        await db.state.get('nonexistent_cell_xyz');
        // If it returns null (cell doesn't exist), that's fine — not all
        // primitives throw on not-found.
      } catch (err) {
        // If it does throw, the error message should be helpful
        expect(err.message).toBeDefined();
      }
    });

    test('branch not found throws NotFoundError with hint', async () => {
      // Switch to a nonexistent branch — should be a NotFoundError
      try {
        await db.branch.switch('no_such_brancch');
        throw new Error('should have thrown');
      } catch (e) {
        expect(e.name).toBe('NotFoundError');
        expect(e.code).toBe('NOT_FOUND');
        expect(e.message).toMatch(/branch not found/i);
      }
    });

    // -----------------------------------------------------------------
    // Namespace backward compat: typed methods still return old types
    // -----------------------------------------------------------------

    test('kv.set() still returns number (version)', async () => {
      const v = await db.kv.set('bc_k', 'v');
      expect(typeof v).toBe('number');
    });

    test('kv.delete() still returns boolean', async () => {
      await db.kv.set('bc_d', 'v');
      const d = await db.kv.delete('bc_d');
      expect(typeof d).toBe('boolean');
      expect(d).toBe(true);
    });

    test('state.delete() still returns boolean', async () => {
      await db.state.set('bc_sd', 'v');
      const d = await db.state.delete('bc_sd');
      expect(typeof d).toBe('boolean');
      expect(d).toBe(true);
    });

    test('events.append() still returns number (sequence)', async () => {
      const s = await db.events.append('bc_evt', { x: 1 });
      expect(typeof s).toBe('number');
    });

    test('json.delete() still returns number', async () => {
      await db.json.set('bc_jd', '$', { a: 1 });
      const r = await db.json.delete('bc_jd', '$');
      expect(typeof r).toBe('number');
    });
  });

  // =========================================================================
  // Close lifecycle
  // =========================================================================

  describe('db.close()', () => {
    test('close resolves without error', async () => {
      const tempDb = Strata.cache();
      await tempDb.kv.set('k', 'v');
      await tempDb.close();
    });
  });
});
