using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using NzbWebDAV.Cache;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Config;
using NzbWebDAV.Streams;
using Serilog;
using UsenetSharp.Models;
using UsenetSharp.Streams;

namespace NzbWebDAV.Clients.Usenet;

/// <summary>
/// Persistent article cache decorator backed by SQLite index + pack files.
/// Stores decoded article bytes in append-only pack files (~512MB each)
/// indexed by a SQLite database, replacing millions of individual files
/// with thousands of packs + one DB.
///
/// Legacy file-per-article cache entries are migrated lazily on access
/// and cleaned up in the background.
/// </summary>
public class PersistentArticleCacheNntpClient : WrappingNntpClient
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _pendingRequests = new();
    private readonly ConcurrentDictionary<string, ArticleCacheDb.CacheEntry> _cachedSegments = new();
    private readonly ArticleCacheDb _db;
    private readonly PackFileManager _packManager;
    private readonly string _cacheDir;
    private readonly LegacyCacheMigrator _legacyMigrator;

    // Batch touch: collect accessed hashes and flush periodically
    private readonly ConcurrentDictionary<string, byte> _touchBuffer = new();
    private readonly Timer _touchFlushTimer;

    // Legacy cleanup state
    private int _legacyCleanupShardIndex;

    public PersistentArticleCacheNntpClient(INntpClient usenetClient, ConfigManager configManager)
        : base(usenetClient)
    {
        _cacheDir = configManager.GetArticleCacheDir();
        _db = new ArticleCacheDb(_cacheDir);
        _packManager = new PackFileManager(_cacheDir);
        _legacyMigrator = new LegacyCacheMigrator(_cacheDir, _db, _packManager);

        // Flush touch buffer every 30 seconds + clean one legacy shard per tick
        _touchFlushTimer = new Timer(_ => OnTimerTick(), null,
            TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

        Task.Run(LoadCacheIndex);
    }

    private void LoadCacheIndex()
    {
        try
        {
            var entries = _db.LoadAll();
            foreach (var (hash, entry) in entries)
                _cachedSegments.TryAdd(hash, entry);

            if (entries.Count > 0)
                Log.Information("Article cache ready: {Count} entries indexed", entries.Count);
        }
        catch (Exception e)
        {
            Log.Error(e, "Failed to load article cache index");
        }
    }

    public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
        SegmentId segmentId, CancellationToken cancellationToken)
    {
        return DecodedBodyAsync(segmentId, onConnectionReadyAgain: null, cancellationToken);
    }

    public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
        SegmentId segmentId, CancellationToken cancellationToken)
    {
        return DecodedArticleAsync(segmentId, onConnectionReadyAgain: null, cancellationToken);
    }

    public override async Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
        SegmentId segmentId, Action<ArticleBodyResult>? onConnectionReadyAgain, CancellationToken cancellationToken)
    {
        var hash = GetHash(segmentId);

        // Fast path: in-memory cache
        if (_cachedSegments.TryGetValue(hash, out var cachedEntry))
        {
            TouchEntry(hash);
            onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
            return ReadCachedBody(segmentId, cachedEntry);
        }

        var semaphore = _pendingRequests.GetOrAdd(hash, _ => new SemaphoreSlim(1, 1));

        try
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            onConnectionReadyAgain?.Invoke(ArticleBodyResult.NotRetrieved);
            throw;
        }

        try
        {
            // Re-check after lock
            if (_cachedSegments.TryGetValue(hash, out var existingEntry))
            {
                TouchEntry(hash);
                onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
                return ReadCachedBody(segmentId, existingEntry);
            }

            // Check legacy cache — migrate on access
            var legacyEntry = _legacyMigrator.TryMigrateEntry(hash);
            if (legacyEntry != null)
            {
                _cachedSegments.TryAdd(hash, legacyEntry);
                _pendingRequests.TryRemove(hash, out _);
                onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
                return ReadCachedBody(segmentId, legacyEntry);
            }

            // Fetch from usenet and cache
            var response = await base.DecodedBodyAsync(segmentId, onConnectionReadyAgain, cancellationToken)
                .ConfigureAwait(false);

            await using var stream = response.Stream;
            var yencHeaders = await stream.GetYencHeadersAsync(cancellationToken).ConfigureAwait(false);
            if (yencHeaders == null)
                throw new InvalidOperationException($"Failed to read yenc headers for segment {segmentId}");

            var (packId, offset, length) = await _packManager.AppendAsync(stream, cancellationToken)
                .ConfigureAwait(false);

            _db.Insert(hash, packId, offset, length, yencHeaders, articleHeaders: null);

            var entry = new ArticleCacheDb.CacheEntry(
                packId, offset, length, yencHeaders,
                HasArticleHeaders: false, ArticleHeaders: null);
            _cachedSegments.TryAdd(hash, entry);
            _pendingRequests.TryRemove(hash, out _);

            return ReadCachedBody(segmentId, entry);
        }
        finally
        {
            semaphore.Release();
        }
    }

    public override async Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
        SegmentId segmentId, Action<ArticleBodyResult>? onConnectionReadyAgain, CancellationToken cancellationToken)
    {
        var hash = GetHash(segmentId);

        // Fast path: cached with full headers
        if (_cachedSegments.TryGetValue(hash, out var fastEntry) && fastEntry.HasArticleHeaders)
        {
            TouchEntry(hash);
            onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
            return ReadCachedArticle(segmentId, fastEntry);
        }

        var semaphore = _pendingRequests.GetOrAdd(hash, _ => new SemaphoreSlim(1, 1));

        try
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            onConnectionReadyAgain?.Invoke(ArticleBodyResult.NotRetrieved);
            throw;
        }

        try
        {
            // Re-check after lock
            if (_cachedSegments.TryGetValue(hash, out var cacheEntry))
            {
                TouchEntry(hash);

                if (cacheEntry.HasArticleHeaders)
                {
                    onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
                    return ReadCachedArticle(segmentId, cacheEntry);
                }

                // Body cached but missing article headers — fetch them
                UsenetHeadResponse? headResponse = null;
                try
                {
                    headResponse = await base.HeadAsync(segmentId, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
                }

                var updatedEntry = cacheEntry with
                {
                    HasArticleHeaders = true,
                    ArticleHeaders = headResponse.ArticleHeaders
                };

                _cachedSegments.TryUpdate(hash, updatedEntry, cacheEntry);
                _db.UpdateArticleHeaders(hash, headResponse.ArticleHeaders!);

                return ReadCachedArticle(segmentId, updatedEntry);
            }

            // Check legacy cache — migrate on access
            var legacyEntry = _legacyMigrator.TryMigrateEntry(hash);
            if (legacyEntry != null)
            {
                _cachedSegments.TryAdd(hash, legacyEntry);
                _pendingRequests.TryRemove(hash, out _);
                onConnectionReadyAgain?.Invoke(ArticleBodyResult.Retrieved);
                return ReadCachedArticle(segmentId, legacyEntry);
            }

            // Fetch from usenet and cache full article
            var response = await base.DecodedArticleAsync(segmentId, onConnectionReadyAgain, cancellationToken)
                .ConfigureAwait(false);

            await using var stream = response.Stream;
            var yencHeaders = await stream.GetYencHeadersAsync(cancellationToken).ConfigureAwait(false);
            if (yencHeaders == null)
                throw new InvalidOperationException($"Failed to read yenc headers for segment {segmentId}");

            var (packId, offset, length) = await _packManager.AppendAsync(stream, cancellationToken)
                .ConfigureAwait(false);

            _db.Insert(hash, packId, offset, length, yencHeaders, response.ArticleHeaders);

            var newEntry = new ArticleCacheDb.CacheEntry(
                packId, offset, length, yencHeaders,
                HasArticleHeaders: true, ArticleHeaders: response.ArticleHeaders);
            _cachedSegments.TryAdd(hash, newEntry);
            _pendingRequests.TryRemove(hash, out _);

            return ReadCachedArticle(segmentId, newEntry);
        }
        finally
        {
            semaphore.Release();
        }
    }

    public override async Task<UsenetExclusiveConnection> AcquireExclusiveConnectionAsync(
        string segmentId, CancellationToken cancellationToken)
    {
        var hash = GetHash(segmentId);

        if (_cachedSegments.ContainsKey(hash))
        {
            TouchEntry(hash);
            return new UsenetExclusiveConnection(onConnectionReadyAgain: null);
        }

        var semaphore = _pendingRequests.GetOrAdd(hash, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cachedSegments.ContainsKey(hash))
            {
                TouchEntry(hash);
                return new UsenetExclusiveConnection(onConnectionReadyAgain: null);
            }

            return await base.AcquireExclusiveConnectionAsync(segmentId, cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    }

    public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
        SegmentId segmentId, UsenetExclusiveConnection exclusiveConnection, CancellationToken cancellationToken)
    {
        return DecodedBodyAsync(segmentId, exclusiveConnection.OnConnectionReadyAgain, cancellationToken);
    }

    public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
        SegmentId segmentId, UsenetExclusiveConnection exclusiveConnection, CancellationToken cancellationToken)
    {
        return DecodedArticleAsync(segmentId, exclusiveConnection.OnConnectionReadyAgain, cancellationToken);
    }

    public override Task<UsenetYencHeader> GetYencHeadersAsync(string segmentId, CancellationToken ct)
    {
        var hash = GetHash(segmentId);
        return _cachedSegments.TryGetValue(hash, out var existingEntry)
            ? Task.FromResult(existingEntry.YencHeaders)
            : base.GetYencHeadersAsync(segmentId, ct);
    }

    /// <summary>
    /// Remove evicted entries from the in-memory dictionary.
    /// Called by ArticleCacheCleanupService after eviction.
    /// </summary>
    public void RemoveEntries(IEnumerable<string> hashes)
    {
        foreach (var hash in hashes)
        {
            _cachedSegments.TryRemove(hash, out _);
            _pendingRequests.TryRemove(hash, out _);
        }
    }

    /// <summary>
    /// Expose the DB and pack manager for the cleanup service.
    /// </summary>
    internal ArticleCacheDb CacheDb => _db;
    internal PackFileManager PackManager => _packManager;

    private UsenetDecodedBodyResponse ReadCachedBody(string segmentId, ArticleCacheDb.CacheEntry entry)
    {
        var dataStream = _packManager.Read(entry.PackId, entry.Offset, entry.Length);
        return new UsenetDecodedBodyResponse
        {
            SegmentId = segmentId,
            ResponseCode = (int)UsenetResponseType.ArticleRetrievedBodyFollows,
            ResponseMessage = "222 - Article retrieved from persistent cache",
            Stream = new CachedYencStream(entry.YencHeaders, dataStream)
        };
    }

    private UsenetDecodedArticleResponse ReadCachedArticle(string segmentId, ArticleCacheDb.CacheEntry entry)
    {
        var dataStream = _packManager.Read(entry.PackId, entry.Offset, entry.Length);
        return new UsenetDecodedArticleResponse
        {
            SegmentId = segmentId,
            ResponseCode = (int)UsenetResponseType.ArticleRetrievedHeadAndBodyFollow,
            ResponseMessage = "220 - Article retrieved from persistent cache",
            ArticleHeaders = entry.ArticleHeaders,
            Stream = new CachedYencStream(entry.YencHeaders, dataStream)
        };
    }

    private void TouchEntry(string hash)
    {
        _touchBuffer.TryAdd(hash, 0);
    }

    private void OnTimerTick()
    {
        FlushTouchBuffer();
        CleanupLegacyShard();
    }

    private void FlushTouchBuffer()
    {
        try
        {
            var hashes = _touchBuffer.Keys.ToList();
            if (hashes.Count == 0) return;

            foreach (var hash in hashes)
                _touchBuffer.TryRemove(hash, out _);

            _db.TouchBatch(hashes);
        }
        catch (Exception e)
        {
            Log.Warning(e, "Failed to flush touch buffer");
        }
    }

    private void CleanupLegacyShard()
    {
        if (!_legacyMigrator.HasLegacyCache) return;
        if (_legacyCleanupShardIndex < 0) return;

        try
        {
            _legacyCleanupShardIndex = _legacyMigrator.CleanupStaleShard(_legacyCleanupShardIndex);
        }
        catch (Exception e)
        {
            Log.Warning(e, "Error during legacy cache cleanup");
            _legacyCleanupShardIndex = -1;
        }
    }

    private static string GetHash(string segmentId)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(segmentId));
        return Convert.ToHexString(hash);
    }

    public override void Dispose()
    {
        _touchFlushTimer.Dispose();
        FlushTouchBuffer();

        foreach (var semaphore in _pendingRequests.Values)
            semaphore.Dispose();

        _pendingRequests.Clear();
        _cachedSegments.Clear();
        _packManager.Dispose();
        _db.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }
}
