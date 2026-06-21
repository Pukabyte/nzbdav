using System.Text.Json;
using Microsoft.Data.Sqlite;
using Serilog;
using UsenetSharp.Models;

namespace NzbWebDAV.Cache;

/// <summary>
/// SQLite-backed index for the article cache. Replaces millions of individual
/// .meta files with a single database, enabling fast startup and efficient
/// LRU eviction queries.
/// </summary>
public sealed class ArticleCacheDb : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { IncludeFields = true };
    private readonly SqliteConnection _connection;
    private readonly Lock _writeLock = new();

    public ArticleCacheDb(string cacheDir)
    {
        var dbPath = Path.Combine(cacheDir, "cache-index.db");
        Directory.CreateDirectory(cacheDir);

        _connection = new SqliteConnection($"Data Source={dbPath};Pooling=false");
        _connection.Open();

        Initialize();
    }

    private void Initialize()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-65536;
            PRAGMA temp_store=MEMORY;

            CREATE TABLE IF NOT EXISTS articles (
                hash TEXT PRIMARY KEY,
                pack_id TEXT NOT NULL,
                offset INTEGER NOT NULL,
                length INTEGER NOT NULL,
                yenc_headers TEXT NOT NULL,
                article_headers TEXT,
                last_accessed INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_articles_last_accessed
                ON articles(last_accessed);

            CREATE INDEX IF NOT EXISTS idx_articles_pack_id
                ON articles(pack_id);
        """;
        cmd.ExecuteNonQuery();
    }

    public record CacheEntry(
        string PackId,
        long Offset,
        long Length,
        UsenetYencHeader YencHeaders,
        bool HasArticleHeaders,
        UsenetArticleHeader? ArticleHeaders);

    /// <summary>
    /// Load all entries into memory at startup. Single sequential read
    /// of the SQLite DB — orders of magnitude faster than enumerating
    /// millions of .meta files.
    /// </summary>
    public Dictionary<string, CacheEntry> LoadAll()
    {
        var result = new Dictionary<string, CacheEntry>();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT hash, pack_id, offset, length, yenc_headers, article_headers, last_accessed FROM articles";
        using var reader = cmd.ExecuteReader();
        var skipped = 0;

        while (reader.Read())
        {
            try
            {
                var hash = reader.GetString(0);
                var packId = reader.GetString(1);
                var offset = reader.GetInt64(2);
                var length = reader.GetInt64(3);
                var yencJson = reader.GetString(4);
                var articleJson = reader.IsDBNull(5) ? null : reader.GetString(5);

                var yencHeaders = JsonSerializer.Deserialize<UsenetYencHeader>(yencJson, JsonOptions);
                if (yencHeaders == null || yencHeaders.PartSize == 0)
                {
                    skipped++;
                    continue;
                }

                var articleHeaders = articleJson != null
                    ? JsonSerializer.Deserialize<UsenetArticleHeader>(articleJson, JsonOptions)
                    : null;

                result[hash] = new CacheEntry(
                    packId, offset, length, yencHeaders,
                    HasArticleHeaders: articleHeaders != null,
                    ArticleHeaders: articleHeaders);
            }
            catch (JsonException)
            {
                skipped++;
            }
        }

        if (result.Count > 0 || skipped > 0)
            Log.Information("Loaded {Count} entries from article cache DB (skipped {Skipped})", result.Count, skipped);

        return result;
    }

    public void Insert(string hash, string packId, long offset, long length,
        UsenetYencHeader yencHeaders, UsenetArticleHeader? articleHeaders)
    {
        var yencJson = JsonSerializer.Serialize(yencHeaders, JsonOptions);
        var articleJson = articleHeaders != null
            ? JsonSerializer.Serialize(articleHeaders, JsonOptions)
            : null;
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        lock (_writeLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                INSERT OR REPLACE INTO articles
                    (hash, pack_id, offset, length, yenc_headers, article_headers, last_accessed)
                VALUES
                    ($hash, $packId, $offset, $length, $yencJson, $articleJson, $lastAccessed)
            """;
            cmd.Parameters.AddWithValue("$hash", hash);
            cmd.Parameters.AddWithValue("$packId", packId);
            cmd.Parameters.AddWithValue("$offset", offset);
            cmd.Parameters.AddWithValue("$length", length);
            cmd.Parameters.AddWithValue("$yencJson", yencJson);
            cmd.Parameters.AddWithValue("$articleJson", (object?)articleJson ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$lastAccessed", now);
            cmd.ExecuteNonQuery();
        }
    }

    public void UpdateArticleHeaders(string hash, UsenetArticleHeader articleHeaders)
    {
        var articleJson = JsonSerializer.Serialize(articleHeaders, JsonOptions);

        lock (_writeLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "UPDATE articles SET article_headers = $articleJson WHERE hash = $hash";
            cmd.Parameters.AddWithValue("$hash", hash);
            cmd.Parameters.AddWithValue("$articleJson", articleJson);
            cmd.ExecuteNonQuery();
        }
    }

    /// <summary>
    /// Batch update last_accessed timestamps. Called periodically instead
    /// of per-access to avoid write amplification.
    /// </summary>
    public void TouchBatch(IReadOnlyCollection<string> hashes)
    {
        if (hashes.Count == 0) return;
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        lock (_writeLock)
        {
            using var savepointCmd = _connection.CreateCommand();
            savepointCmd.CommandText = "SAVEPOINT touch";
            savepointCmd.ExecuteNonQuery();

            try
            {
                using var cmd = _connection.CreateCommand();
                cmd.CommandText = "UPDATE articles SET last_accessed = $now WHERE hash = $hash";
                var hashParam = cmd.Parameters.Add("$hash", SqliteType.Text);
                cmd.Parameters.AddWithValue("$now", now);
                cmd.Prepare();

                foreach (var hash in hashes)
                {
                    hashParam.Value = hash;
                    cmd.ExecuteNonQuery();
                }

                using var releaseCmd = _connection.CreateCommand();
                releaseCmd.CommandText = "RELEASE SAVEPOINT touch";
                releaseCmd.ExecuteNonQuery();
            }
            catch
            {
                using var rollbackCmd = _connection.CreateCommand();
                rollbackCmd.CommandText = "ROLLBACK TO SAVEPOINT touch";
                rollbackCmd.ExecuteNonQuery();
                using var releaseCmd = _connection.CreateCommand();
                releaseCmd.CommandText = "RELEASE SAVEPOINT touch";
                releaseCmd.ExecuteNonQuery();
                throw;
            }
        }
    }

    /// <summary>
    /// Get total cache size in bytes.
    /// </summary>
    public long GetTotalSize()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT COALESCE(SUM(length), 0) FROM articles";
        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>
    /// Get the oldest entries by last_accessed for eviction.
    /// Returns (hash, packId, length) tuples.
    /// </summary>
    public List<(string Hash, string PackId, long Length)> GetOldestEntries(long bytesToEvict)
    {
        var result = new List<(string, string, long)>();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT hash, pack_id, length FROM articles ORDER BY last_accessed ASC";
        using var reader = cmd.ExecuteReader();

        long accumulated = 0;
        while (reader.Read() && accumulated < bytesToEvict)
        {
            var hash = reader.GetString(0);
            var packId = reader.GetString(1);
            var length = reader.GetInt64(2);
            result.Add((hash, packId, length));
            accumulated += length;
        }

        return result;
    }

    /// <summary>
    /// Remove entries by hash. Returns the set of pack IDs affected.
    /// Uses SAVEPOINT to avoid "cannot start a transaction within a transaction"
    /// errors when implicit read transactions are active on the shared connection.
    /// </summary>
    public HashSet<string> RemoveEntries(IReadOnlyCollection<string> hashes)
    {
        var affectedPacks = new HashSet<string>();
        if (hashes.Count == 0) return affectedPacks;

        lock (_writeLock)
        {
            using var savepointCmd = _connection.CreateCommand();
            savepointCmd.CommandText = "SAVEPOINT evict";
            savepointCmd.ExecuteNonQuery();

            try
            {
                // First collect affected pack IDs
                using (var selectCmd = _connection.CreateCommand())
                {
                    selectCmd.CommandText = "SELECT DISTINCT pack_id FROM articles WHERE hash = $hash";
                    var hashParam = selectCmd.Parameters.Add("$hash", SqliteType.Text);
                    foreach (var hash in hashes)
                    {
                        hashParam.Value = hash;
                        using var reader = selectCmd.ExecuteReader();
                        while (reader.Read())
                            affectedPacks.Add(reader.GetString(0));
                    }
                }

                // Then delete
                using (var deleteCmd = _connection.CreateCommand())
                {
                    deleteCmd.CommandText = "DELETE FROM articles WHERE hash = $hash";
                    var hashParam = deleteCmd.Parameters.Add("$hash", SqliteType.Text);
                    deleteCmd.Prepare();
                    foreach (var hash in hashes)
                    {
                        hashParam.Value = hash;
                        deleteCmd.ExecuteNonQuery();
                    }
                }

                using var releaseCmd = _connection.CreateCommand();
                releaseCmd.CommandText = "RELEASE SAVEPOINT evict";
                releaseCmd.ExecuteNonQuery();
            }
            catch
            {
                using var rollbackCmd = _connection.CreateCommand();
                rollbackCmd.CommandText = "ROLLBACK TO SAVEPOINT evict";
                rollbackCmd.ExecuteNonQuery();
                using var releaseCmd = _connection.CreateCommand();
                releaseCmd.CommandText = "RELEASE SAVEPOINT evict";
                releaseCmd.ExecuteNonQuery();
                throw;
            }
        }

        return affectedPacks;
    }

    /// <summary>
    /// Get all entries in a specific pack file. Used during compaction.
    /// </summary>
    public List<(string Hash, long Offset, long Length)> GetEntriesInPack(string packId)
    {
        var result = new List<(string, long, long)>();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT hash, offset, length FROM articles WHERE pack_id = $packId ORDER BY offset ASC";
        cmd.Parameters.AddWithValue("$packId", packId);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            result.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
        return result;
    }

    /// <summary>
    /// Update pack location for an entry (used during compaction).
    /// </summary>
    public void UpdateLocation(string hash, string newPackId, long newOffset)
    {
        lock (_writeLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "UPDATE articles SET pack_id = $packId, offset = $offset WHERE hash = $hash";
            cmd.Parameters.AddWithValue("$hash", hash);
            cmd.Parameters.AddWithValue("$packId", newPackId);
            cmd.Parameters.AddWithValue("$offset", newOffset);
            cmd.ExecuteNonQuery();
        }
    }

    public long GetEntryCount()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM articles";
        return (long)cmd.ExecuteScalar()!;
    }

    public void Dispose()
    {
        _connection.Close();
        _connection.Dispose();
    }
}
