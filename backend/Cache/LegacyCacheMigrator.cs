using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Serilog;
using UsenetSharp.Models;

namespace NzbWebDAV.Cache;

/// <summary>
/// Lazy migration from the legacy file-per-article cache format.
/// Instead of bulk migration on startup, entries are migrated on-access:
/// cache miss in SQLite -> check legacy files -> if found, pack + index + delete old files.
/// A background cleanup removes stale legacy files past a configurable TTL.
/// </summary>
public sealed class LegacyCacheMigrator
{
    private static readonly JsonSerializerOptions JsonOptions = new() { IncludeFields = true };
    private static readonly TimeSpan StaleTtl = TimeSpan.FromDays(30);

    private record CacheMetadata(
        UsenetYencHeader YencHeaders,
        UsenetArticleHeader? ArticleHeaders);

    private readonly string _cacheDir;
    private readonly ArticleCacheDb _db;
    private readonly PackFileManager _packManager;
    private readonly bool _hasLegacyCache;

    public LegacyCacheMigrator(string cacheDir, ArticleCacheDb db, PackFileManager packManager)
    {
        _cacheDir = cacheDir;
        _db = db;
        _packManager = packManager;
        _hasLegacyCache = DetectLegacyCache(cacheDir);

        if (_hasLegacyCache)
            Log.Information("Legacy article cache detected — entries will be migrated on access");
    }

    public bool HasLegacyCache => _hasLegacyCache;

    /// <summary>
    /// Try to find and migrate a single entry from the legacy cache.
    /// Called on cache miss in the main lookup path.
    /// Returns the new CacheEntry if found and migrated, null otherwise.
    /// </summary>
    public ArticleCacheDb.CacheEntry? TryMigrateEntry(string hash)
    {
        if (!_hasLegacyCache) return null;

        var prefix = hash[..2];
        var legacyDataPath = Path.Combine(_cacheDir, prefix, hash);
        var legacyMetaPath = legacyDataPath + ".meta";

        if (!File.Exists(legacyDataPath) || !File.Exists(legacyMetaPath))
            return null;

        try
        {
            // Read metadata
            var metaJson = File.ReadAllText(legacyMetaPath);
            var metadata = JsonSerializer.Deserialize<CacheMetadata>(metaJson, JsonOptions);
            if (metadata?.YencHeaders == null || metadata.YencHeaders.PartSize == 0)
            {
                TryDeleteFile(legacyDataPath);
                TryDeleteFile(legacyMetaPath);
                return null;
            }

            // Pack the data
            using var dataStream = new FileStream(legacyDataPath, FileMode.Open,
                FileAccess.Read, FileShare.Read, 81920);
            var (packId, offset, length) = _packManager
                .AppendAsync(dataStream, CancellationToken.None)
                .GetAwaiter().GetResult();

            // Index in SQLite
            _db.Insert(hash, packId, offset, length,
                metadata.YencHeaders, metadata.ArticleHeaders);

            // Delete old files immediately
            dataStream.Dispose();
            TryDeleteFile(legacyDataPath);
            TryDeleteFile(legacyMetaPath);
            TryDeleteEmptyDirectory(Path.Combine(_cacheDir, prefix));

            return new ArticleCacheDb.CacheEntry(
                packId, offset, length, metadata.YencHeaders,
                HasArticleHeaders: metadata.ArticleHeaders != null,
                ArticleHeaders: metadata.ArticleHeaders);
        }
        catch (JsonException)
        {
            TryDeleteFile(legacyDataPath);
            TryDeleteFile(legacyMetaPath);
            return null;
        }
        catch (Exception e)
        {
            Log.Warning(e, "Failed to lazily migrate cache entry {Hash}", hash);
            return null;
        }
    }

    /// <summary>
    /// Background cleanup: delete legacy files that haven't been accessed
    /// within StaleTtl. Processes one shard per call to avoid IO spikes.
    /// Returns the shard index to resume from next time, or -1 if done.
    /// </summary>
    public int CleanupStaleShard(int shardIndex)
    {
        if (!_hasLegacyCache) return -1;

        var shardDirs = GetLegacyShardDirs();
        if (shardIndex >= shardDirs.Count) return -1;

        var shardDir = shardDirs[shardIndex];
        var cutoff = DateTime.UtcNow - StaleTtl;
        var deleted = 0;

        try
        {
            var files = Directory.EnumerateFiles(shardDir)
                .Where(f => !f.EndsWith(".meta") && !f.EndsWith(".tmp"))
                .ToList();

            foreach (var dataFile in files)
            {
                try
                {
                    var info = new FileInfo(dataFile);
                    if (info.LastAccessTimeUtc < cutoff)
                    {
                        TryDeleteFile(dataFile);
                        TryDeleteFile(dataFile + ".meta");
                        deleted++;
                    }
                }
                catch { /* skip */ }
            }

            TryDeleteEmptyDirectory(shardDir);
        }
        catch (Exception e)
        {
            Log.Warning(e, "Error cleaning stale legacy shard {ShardDir}", shardDir);
        }

        if (deleted > 0)
            Log.Information("Cleaned {Count} stale legacy cache entries from shard {Shard}",
                deleted, Path.GetFileName(shardDir));

        return shardIndex + 1 < shardDirs.Count ? shardIndex + 1 : -1;
    }

    private List<string> GetLegacyShardDirs()
    {
        try
        {
            return Directory.GetDirectories(_cacheDir)
                .Where(d =>
                {
                    var name = Path.GetFileName(d);
                    return name is { Length: 2 }
                        && name != "pa"
                        && name.All(c => "0123456789ABCDEFabcdef".Contains(c));
                })
                .OrderBy(d => d)
                .ToList();
        }
        catch { return []; }
    }

    private static bool DetectLegacyCache(string cacheDir)
    {
        if (!Directory.Exists(cacheDir)) return false;
        try
        {
            return Directory.EnumerateDirectories(cacheDir)
                .Select(Path.GetFileName)
                .Any(name => name is { Length: 2 }
                    && name != "pa"
                    && name.All(c => "0123456789ABCDEFabcdef".Contains(c)));
        }
        catch { return false; }
    }

    private static void TryDeleteFile(string path)
    {
        try { File.Delete(path); }
        catch { /* best-effort */ }
    }

    private static void TryDeleteEmptyDirectory(string? directory)
    {
        if (string.IsNullOrEmpty(directory)) return;
        try
        {
            if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                Directory.Delete(directory, recursive: false);
        }
        catch { /* best-effort */ }
    }
}
