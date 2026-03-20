using Serilog;

namespace NzbWebDAV.Cache;

/// <summary>
/// Manages append-only pack files for article data storage.
/// Articles are appended sequentially into large files (~512MB each),
/// reducing millions of individual files to thousands of pack files.
/// </summary>
public sealed class PackFileManager : IDisposable
{
    private const long MaxPackSize = 512L * 1024 * 1024; // 512MB per pack
    private const int ReadBufferSize = 81920;

    private readonly string _cacheDir;
    private readonly Lock _writeLock = new();
    private string? _activePackId;
    private FileStream? _activePackStream;

    public PackFileManager(string cacheDir)
    {
        _cacheDir = Path.Combine(cacheDir, "packs");
        Directory.CreateDirectory(_cacheDir);
    }

    /// <summary>
    /// Append article data to the current pack file.
    /// Returns (packId, offset, length) for index storage.
    /// </summary>
    public async Task<(string PackId, long Offset, long Length)> AppendAsync(
        Stream data, CancellationToken ct)
    {
        // Read the data into memory first so we know the length
        // and can write it atomically to the pack file.
        using var memStream = new MemoryStream();
        await data.CopyToAsync(memStream, ct).ConfigureAwait(false);
        var bytes = memStream.ToArray();

        lock (_writeLock)
        {
            EnsureActivePack(bytes.Length);
            var offset = _activePackStream!.Position;
            _activePackStream.Write(bytes);
            _activePackStream.Flush();
            return (_activePackId!, offset, bytes.Length);
        }
    }

    /// <summary>
    /// Read article data from a pack file at the given offset and length.
    /// Returns a stream positioned at the data.
    /// </summary>
    public Stream Read(string packId, long offset, long length)
    {
        var packPath = GetPackPath(packId);
        var fileStream = new FileStream(packPath, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite, ReadBufferSize, useAsync: true);
        return new PackSliceStream(fileStream, offset, length);
    }

    /// <summary>
    /// Delete a pack file entirely (used after compaction or when all entries evicted).
    /// </summary>
    public void DeletePack(string packId)
    {
        var packPath = GetPackPath(packId);
        try
        {
            lock (_writeLock)
            {
                // If this is the active pack, close it
                if (_activePackId == packId)
                {
                    _activePackStream?.Dispose();
                    _activePackStream = null;
                    _activePackId = null;
                }
            }

            if (File.Exists(packPath))
                File.Delete(packPath);
        }
        catch (Exception e)
        {
            Log.Warning(e, "Failed to delete pack file {PackId}", packId);
        }
    }

    /// <summary>
    /// Get the actual disk size of a pack file.
    /// </summary>
    public long GetPackFileSize(string packId)
    {
        var packPath = GetPackPath(packId);
        try
        {
            return File.Exists(packPath) ? new FileInfo(packPath).Length : 0;
        }
        catch
        {
            return 0;
        }
    }

    /// <summary>
    /// Get all pack file IDs on disk.
    /// </summary>
    public List<string> GetAllPackIds()
    {
        if (!Directory.Exists(_cacheDir)) return [];
        return Directory.EnumerateFiles(_cacheDir, "*.pack")
            .Select(Path.GetFileNameWithoutExtension)
            .Where(id => id != null)
            .Select(id => id!)
            .ToList();
    }

    private void EnsureActivePack(long requiredSpace)
    {
        if (_activePackStream != null && _activePackStream.Position + requiredSpace <= MaxPackSize)
            return;

        // Close current pack if it exists
        _activePackStream?.Dispose();

        // Create new pack
        _activePackId = Guid.NewGuid().ToString("N")[..16];
        var packPath = GetPackPath(_activePackId);
        _activePackStream = new FileStream(packPath, FileMode.CreateNew, FileAccess.Write,
            FileShare.Read, ReadBufferSize);
    }

    private string GetPackPath(string packId)
    {
        return Path.Combine(_cacheDir, $"{packId}.pack");
    }

    public void Dispose()
    {
        _activePackStream?.Dispose();
    }

    /// <summary>
    /// A read-only stream that exposes a slice of a larger file.
    /// </summary>
    private sealed class PackSliceStream : Stream
    {
        private readonly FileStream _fileStream;
        private readonly long _sliceStart;
        private readonly long _sliceLength;
        private long _position;

        public PackSliceStream(FileStream fileStream, long offset, long length)
        {
            _fileStream = fileStream;
            _sliceStart = offset;
            _sliceLength = length;
            _position = 0;
            _fileStream.Seek(offset, SeekOrigin.Begin);
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => _sliceLength;

        public override long Position
        {
            get => _position;
            set
            {
                ArgumentOutOfRangeException.ThrowIfNegative(value);
                ArgumentOutOfRangeException.ThrowIfGreaterThan(value, _sliceLength);
                _position = value;
                _fileStream.Seek(_sliceStart + value, SeekOrigin.Begin);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var remaining = (int)Math.Min(count, _sliceLength - _position);
            if (remaining <= 0) return 0;
            var read = _fileStream.Read(buffer, offset, remaining);
            _position += read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            var remaining = (int)Math.Min(buffer.Length, _sliceLength - _position);
            if (remaining <= 0) return 0;
            var read = await _fileStream.ReadAsync(buffer[..remaining], ct).ConfigureAwait(false);
            _position += read;
            return read;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            var remaining = (int)Math.Min(count, _sliceLength - _position);
            if (remaining <= 0) return 0;
            var read = await _fileStream.ReadAsync(buffer, offset, remaining, ct).ConfigureAwait(false);
            _position += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var newPos = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End => _sliceLength + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };
            Position = newPos;
            return _position;
        }

        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() { }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _fileStream.Dispose();
            base.Dispose(disposing);
        }
    }
}
