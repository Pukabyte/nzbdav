using Microsoft.Extensions.Hosting;
using NzbWebDAV.Cache;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using Serilog;

namespace NzbWebDAV.Services;

/// <summary>
/// Background service that periodically evicts the oldest cached articles
/// when the persistent article cache exceeds the configured maximum size.
/// Uses SQLite index for fast LRU queries instead of filesystem enumeration.
/// After eviction, compacts fragmented pack files to reclaim disk space.
/// </summary>
public class ArticleCacheCleanupService(
    ConfigManager configManager,
    UsenetStreamingClient streamingClient
) : BackgroundService
{
    private static readonly TimeSpan CleanupInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Packs with less than this ratio of live data will be compacted.
    /// e.g. 0.5 means if a 512MB pack has less than 256MB of live data, compact it.
    /// </summary>
    private const double CompactionThreshold = 0.5;

    /// <summary>
    /// Maximum number of packs to compact per cycle to avoid I/O storms.
    /// </summary>
    private const int MaxPacksPerCompaction = 50;

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
                var cacheClient = FindPersistentCacheClient(streamingClient);
                if (cacheClient == null) continue;

                EvictIfOverSize(maxSizeBytes, cacheClient);
                await CompactFragmentedPacks(cacheClient, stoppingToken).ConfigureAwait(false);
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

    private static void EvictIfOverSize(long maxSizeBytes, PersistentArticleCacheNntpClient cacheClient)
    {
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

    /// <summary>
    /// Find pack files with significant dead space and rewrite their live entries
    /// into new packs, then delete the old fragmented packs.
    /// </summary>
    private static async Task CompactFragmentedPacks(
        PersistentArticleCacheNntpClient cacheClient, CancellationToken ct)
    {
        var db = cacheClient.CacheDb;
        var packManager = cacheClient.PackManager;

        var allPackIds = packManager.GetAllPackIds();
        if (allPackIds.Count == 0) return;

        // Find packs where live data ratio is below threshold
        var fragmentedPacks = new List<(string PackId, long FileSize, long LiveSize, int EntryCount)>();

        foreach (var packId in allPackIds)
        {
            if (ct.IsCancellationRequested) return;

            var fileSize = packManager.GetPackFileSize(packId);
            if (fileSize <= 0) continue;

            var entries = db.GetEntriesInPack(packId);
            if (entries.Count == 0)
            {
                // Orphaned pack with no index entries — delete it
                packManager.DeletePack(packId);
                continue;
            }

            var liveSize = entries.Sum(e => e.Length);
            var ratio = (double)liveSize / fileSize;

            if (ratio < CompactionThreshold)
                fragmentedPacks.Add((packId, fileSize, liveSize, entries.Count));
        }

        if (fragmentedPacks.Count == 0) return;

        // Sort by worst ratio first (most wasted space)
        fragmentedPacks.Sort((a, b) =>
        {
            var ratioA = (double)a.LiveSize / a.FileSize;
            var ratioB = (double)b.LiveSize / b.FileSize;
            return ratioA.CompareTo(ratioB);
        });

        var packsToCompact = fragmentedPacks.Take(MaxPacksPerCompaction).ToList();
        var totalWasted = packsToCompact.Sum(p => p.FileSize - p.LiveSize);

        Log.Information(
            "Compacting {Count} fragmented packs to reclaim ~{ReclaimMb}MB",
            packsToCompact.Count, totalWasted / (1024 * 1024));

        var totalCompacted = 0;
        var totalReclaimed = 0L;

        foreach (var (packId, fileSize, _, _) in packsToCompact)
        {
            if (ct.IsCancellationRequested) break;

            try
            {
                var reclaimed = await CompactPack(packId, db, packManager, cacheClient, ct)
                    .ConfigureAwait(false);
                if (reclaimed > 0)
                {
                    totalCompacted++;
                    totalReclaimed += reclaimed;
                }
            }
            catch (Exception e)
            {
                Log.Warning(e, "Failed to compact pack {PackId}", packId);
            }
        }

        if (totalCompacted > 0)
        {
            Log.Information(
                "Compaction complete: {Count} packs compacted, {ReclaimedMb}MB reclaimed",
                totalCompacted, totalReclaimed / (1024 * 1024));
        }
    }

    /// <summary>
    /// Compact a single pack: read live entries, write to new packs,
    /// update index, delete old pack.
    /// </summary>
    private static async Task<long> CompactPack(
        string packId,
        ArticleCacheDb db,
        PackFileManager packManager,
        PersistentArticleCacheNntpClient cacheClient,
        CancellationToken ct)
    {
        var entries = db.GetEntriesInPack(packId);
        if (entries.Count == 0)
        {
            packManager.DeletePack(packId);
            return 0;
        }

        var oldFileSize = packManager.GetPackFileSize(packId);

        // Read each live entry and append to new packs
        foreach (var (hash, offset, length) in entries)
        {
            if (ct.IsCancellationRequested) return 0;

            try
            {
                using var dataStream = packManager.Read(packId, offset, length);
                var (newPackId, newOffset, newLength) = await packManager.AppendAsync(dataStream, ct)
                    .ConfigureAwait(false);

                // Update the index to point to the new location
                db.UpdateLocation(hash, newPackId, newOffset);

                // Update in-memory cache entry
                cacheClient.UpdateEntryLocation(hash, newPackId, newOffset);
            }
            catch (Exception e)
            {
                Log.Warning(e, "Failed to relocate entry {Hash} from pack {PackId}", hash, packId);
                return 0; // Abort this pack — don't delete it if entries failed to move
            }
        }

        // All entries moved — delete the old pack
        packManager.DeletePack(packId);
        return oldFileSize;
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
