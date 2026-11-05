import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createCheckpointManager, DEFAULT_HORIZON_CURSOR_KEY } from '../checkpoint';
import type Redis from 'ioredis';

describe('Horizon CheckpointManager', () => {
  let mockStorage: Map<string, string>;
  let fakeRedis: Pick<Redis, 'get' | 'set' | 'del'>;

  beforeEach(() => {
    mockStorage = new Map<string, string>();
    fakeRedis = {
      get: vi.fn((key: string) => Promise.resolve(mockStorage.get(key) ?? null)) as unknown as Redis['get'],
      set: vi.fn((key: string, val: string) => {
        mockStorage.set(key, val);
        return Promise.resolve('OK');
      }) as unknown as Redis['set'],
      del: vi.fn((key: string) => {
        mockStorage.delete(key);
        return Promise.resolve(1);
      }) as unknown as Redis['del'],
    };
  });

  it('loads undefined when no cursor is stored', async () => {
    const manager = createCheckpointManager({ redis: fakeRedis });
    const cursor = await manager.loadCursor();
    expect(cursor).toBeUndefined();
    expect(manager.getCurrentCursor()).toBeUndefined();
    expect(manager.getMetrics().checkpointAgeMs).toBeNull();
  });

  it('loads previously persisted cursor from storage', async () => {
    mockStorage.set(DEFAULT_HORIZON_CURSOR_KEY, '123456789-1');
    const manager = createCheckpointManager({ redis: fakeRedis });
    const cursor = await manager.loadCursor();
    expect(cursor).toBe('123456789-1');
    expect(manager.getCurrentCursor()).toBe('123456789-1');
    expect(manager.getMetrics().checkpointAgeMs).toBeGreaterThanOrEqual(0);
  });

  it('persists cursor automatically when saveInterval threshold is reached', async () => {
    const manager = createCheckpointManager({ redis: fakeRedis, saveInterval: 3 });

    await manager.recordCursor('token-1');
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBeUndefined();
    expect(manager.getMetrics().opsSinceLastSave).toBe(1);

    await manager.recordCursor('token-2');
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBeUndefined();
    expect(manager.getMetrics().opsSinceLastSave).toBe(2);

    // 3rd op triggers automatic persist
    await manager.recordCursor('token-3');
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBe('token-3');
    expect(fakeRedis.set).toHaveBeenCalledTimes(1);
    expect(manager.getMetrics().opsSinceLastSave).toBe(0);
    expect(manager.getMetrics().checkpointAgeMs).not.toBeNull();
  });

  it('flushes cursor immediately on explicit flush call', async () => {
    const manager = createCheckpointManager({ redis: fakeRedis, saveInterval: 50 });
    await manager.recordCursor('token-fast');
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBeUndefined();

    await manager.flush();
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBe('token-fast');
  });

  it('clears stale cursor and increments gap metrics when Horizon history is pruned', async () => {
    mockStorage.set(DEFAULT_HORIZON_CURSOR_KEY, 'expired-token-100');
    const manager = createCheckpointManager({ redis: fakeRedis });
    await manager.loadCursor();
    expect(manager.getCurrentCursor()).toBe('expired-token-100');

    const fallbackCursor = await manager.handleStaleCursor('horizon_410_gone');
    expect(fallbackCursor).toBe('now');
    expect(manager.getCurrentCursor()).toBeUndefined();
    expect(mockStorage.get(DEFAULT_HORIZON_CURSOR_KEY)).toBeUndefined();
    expect(fakeRedis.del).toHaveBeenCalledWith(DEFAULT_HORIZON_CURSOR_KEY);
    expect(manager.getMetrics().gapCount).toBe(1);
  });

  it('tracks explicit ingestion gaps via recordGap', () => {
    const manager = createCheckpointManager({ redis: fakeRedis });
    expect(manager.getMetrics().gapCount).toBe(0);
    manager.recordGap('100-1', '250-1');
    expect(manager.getMetrics().gapCount).toBe(1);
  });

  it('handles read failures gracefully without throwing', async () => {
    fakeRedis.get = vi.fn().mockRejectedValue(new Error('Redis connection refused')) as unknown as Redis['get'];
    const manager = createCheckpointManager({ redis: fakeRedis });
    const cursor = await manager.loadCursor();
    expect(cursor).toBeUndefined();
  });
});
