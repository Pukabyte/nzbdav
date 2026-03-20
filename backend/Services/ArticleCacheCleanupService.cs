using Microsoft.Extensions.Hosting;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using Serilog;

namespace NzbWebDAV.Services;

/// <summary>
/// Background service that periodically evicts the oldest cached articles
/// when the persistent article cache exceeds the configured maximum size.
/// Uses SQLite index for fast LRU queries instead of filesystem enumeration.
/// </summary>
public class ArticleCacheCleanupService(
    ConfigManager configManager,
    UsenetStreamingClient streamingClient
) : BackgroundService
{
    private static readonly TimeSpan CleanupInterval = TimeSpan.FromMinutes(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(CleanupInterval, stoppingToken).ConfigureAwait(false);

                if (!configManager.IsArticleCacheEnabled()) continue;

                var maxSizeGb = configManager.GetArticleCacheMaxSizeGb();
                if (maxSizeGb <= 0) continue;

                var maxSizeBytes = (long)maxSizeGb * 1024 * 1024 * 1024;
                EvictIfOverSize(maxSizeBytes, streamingClient);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                Log.Error(e, "Error during article cache cleanup: {Message}", e.Message);
            }
        }
    }

    private static void EvictIfOverSize(long maxSizeBytes, UsenetStreamingClient streamingClient)
    {
        var cacheClient = FindPersistentCacheClient(streamingClient);
        if (cacheClient == null) return;

        var db = cacheClient.CacheDb;
        var packManager = cacheClient.PackManager;

        var totalSize = db.GetTotalSize();
        if (totalSize <= maxSizeBytes) return;

        var targetSizeBytes = (long)(maxSizeBytes * 0.9);
        var bytesToEvict = totalSize - targetSizeBytes;

        Log.Information(
            "Article cache size {SizeMb}MB exceeds max {MaxMb}MB, evicting {EvictMb}MB",
            totalSize / (1024 * 1024), maxSizeBytes / (1024 * 1024), bytesToEvict / (1024 * 1024));

        // Get oldest entries from SQLite (instant LRU query)
        var entriesToEvict = db.GetOldestEntries(bytesToEvict);
        if (entriesToEvict.Count == 0) return;

        var hashes = entriesToEvict.Select(e => e.Hash).ToList();

        // Remove from DB and get affected pack IDs
        var affectedPacks = db.RemoveEntries(hashes);

        // Remove from in-memory cache
        cacheClient.RemoveEntries(hashes);

        // Clean up empty pack files
        foreach (var packId in affectedPacks)
        {
            var remaining = db.GetEntriesInPack(packId);
            if (remaining.Count == 0)
                packManager.DeletePack(packId);
        }

        Log.Information("Evicted {Count} entries from article cache", entriesToEvict.Count);
    }

    private static PersistentArticleCacheNntpClient? FindPersistentCacheClient(UsenetStreamingClient streamingClient)
    {
        if (streamingClient is not WrappingNntpClient wrapper) return null;

        var current = wrapper;
        while (current != null)
        {
            if (current is PersistentArticleCacheNntpClient cacheClient)
                return cacheClient;

            var field = typeof(WrappingNntpClient).GetField("_usenetClient",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var inner = field?.GetValue(current) as INntpClient;

            if (inner is WrappingNntpClient wrappingInner)
                current = wrappingInner;
            else
                break;
        }

        return null;
    }
}
