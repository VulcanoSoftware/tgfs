import errno
import os

import pytest
from tgfs.app.sftp.handles import (
    SEEK_DATA,
    SEEK_FORWARD_THRESHOLD,
    SEEK_HOLE,
    ReadHandle,
    WriteHandle,
)
from tgfs.core import Ops
from tgfs.reqres import SentFileMessage

CONTENT = bytes(range(256)) * 64  # 16 KiB of easily verifiable data


def make_ops(mocker, content: bytes = CONTENT, chunk_size: int = 1024):
    """An ``Ops`` mock whose ``download`` streams ``content`` from an offset."""
    ops = mocker.Mock(spec=Ops)

    async def download(path, begin, end, as_name):
        stop = len(content) if end < 0 else min(len(content), end + 1)

        async def stream():
            for start in range(begin, stop, chunk_size):
                yield content[start : min(start + chunk_size, stop)]

        return stream()

    ops.download = mocker.AsyncMock(side_effect=download)
    return ops


class TestReadHandle:
    @pytest.fixture
    def ops(self, mocker):
        return make_ops(mocker)

    @pytest.fixture
    def handle(self, ops):
        return ReadHandle(ops, "/file.bin", "file.bin", len(CONTENT))

    async def test_reads_sequentially_across_chunks(self, handle, ops):
        received = b""
        offset = 0
        while chunk := await handle.read(offset, 3000):
            received += chunk
            offset += len(chunk)

        assert received == CONTENT
        assert ops.download.await_count == 1

    async def test_read_returns_at_most_the_requested_size(self, handle):
        assert await handle.read(0, 10) == CONTENT[:10]

    async def test_read_past_the_end_signals_eof(self, handle):
        assert await handle.read(len(CONTENT), 10) == b""

    async def test_read_of_zero_bytes_is_empty(self, handle):
        assert await handle.read(0, 0) == b""

    async def test_backward_seek_reopens_the_stream(self, handle, ops):
        await handle.read(0, 4096)
        assert await handle.read(100, 16) == CONTENT[100:116]

        assert ops.download.await_count == 2
        assert ops.download.await_args.args[1] == 100

    async def test_small_forward_gap_reads_through(self, handle, ops):
        await handle.read(0, 16)
        assert await handle.read(2048, 16) == CONTENT[2048:2064]

        assert ops.download.await_count == 1

    async def test_large_forward_gap_reopens_the_stream(self, mocker):
        content = b"x" * (4 * SEEK_FORWARD_THRESHOLD)
        ops = make_ops(mocker, content, chunk_size=SEEK_FORWARD_THRESHOLD)
        handle = ReadHandle(ops, "/big.bin", "big.bin", len(content))

        await handle.read(0, 16)
        offset = 2 * SEEK_FORWARD_THRESHOLD
        assert len(await handle.read(offset, 16)) == 16

        assert ops.download.await_count == 2
        assert ops.download.await_args.args[1] == offset

    async def test_close_is_idempotent(self, handle):
        await handle.read(0, 16)
        await handle.close()
        await handle.close()


class TestSparseRangeProbing:
    """Clients walk a transfer source with SEEK_DATA/SEEK_HOLE first."""

    @pytest.fixture
    def handle(self, mocker):
        return ReadHandle(make_ops(mocker), "/file.bin", "file.bin", len(CONTENT))

    def test_the_whole_file_reads_as_one_data_region(self, handle):
        assert handle.seek(0, SEEK_DATA) == 0
        assert handle.seek(0, SEEK_HOLE) == len(CONTENT)

    def test_probing_from_the_middle(self, handle):
        assert handle.seek(100, SEEK_DATA) == 100
        assert handle.seek(100, SEEK_HOLE) == len(CONTENT)

    def test_probing_past_the_end_reports_no_more_data(self, handle):
        with pytest.raises(OSError) as excinfo:
            handle.seek(len(CONTENT), SEEK_DATA)

        assert excinfo.value.errno == errno.ENXIO

    def test_ordinary_seeking_is_not_offered(self, handle):
        with pytest.raises(OSError) as excinfo:
            handle.seek(0, os.SEEK_SET)

        assert excinfo.value.errno == errno.ESPIPE


class TestWriteHandle:
    """Tests for the streaming upload fast path and spool fallback."""

    @pytest.fixture
    def ops(self, mocker):
        """``Ops`` mock supporting both streaming and spool upload paths."""
        ops = mocker.Mock(spec=Ops)
        ops.uploaded = {}
        ops.uploaded_parts = []
        ops.deleted_parts = []
        ops.committed = {}
        ops._part_counter = 0

        async def upload_part(data, name):
            ops._part_counter += 1
            sent = SentFileMessage(message_id=1000 + ops._part_counter, size=len(data))
            ops.uploaded_parts.append((name, data))
            return [sent]

        async def upload_from_stream(stream, size, remote):
            data = b"".join([chunk async for chunk in stream])
            assert len(data) == size
            ops.uploaded[remote] = data

        async def delete_uploaded_parts(message_ids):
            ops.deleted_parts.extend(message_ids)

        async def download_parts_back(sent_messages, name):
            """Replay the exact bytes that were uploaded as parts."""
            for name_, data in ops.uploaded_parts:
                yield data

        async def commit_streamed_upload(dirname, name, parts):
            data = b"".join(p.size * b"x" for p in parts)  # not real data
            ops.committed[f"/{dirname}/{name}"] = parts

        ops.upload_part = mocker.AsyncMock(side_effect=upload_part)
        ops.upload_from_stream = mocker.AsyncMock(side_effect=upload_from_stream)
        ops.delete_uploaded_parts = mocker.AsyncMock(side_effect=delete_uploaded_parts)
        ops.download_parts_back = mocker.AsyncMock(side_effect=download_parts_back)
        ops.commit_streamed_upload = mocker.AsyncMock(
            side_effect=commit_streamed_upload
        )
        return ops

    def _handle(self, ops, streaming_part_size=1024, spool_max_bytes=4096):
        return WriteHandle(
            ops,
            "/file.bin",
            streaming_part_size=streaming_part_size,
            spool_max_bytes=spool_max_bytes,
        )

    async def test_sequential_writes_are_streamed_and_committed(self, ops):
        handle = self._handle(ops, streaming_part_size=6)
        await handle.write(0, b"hello ")
        await handle.write(6, b"world")
        await handle.close()

        # First 6 bytes flushed as a part, remaining 5 bytes flushed at close.
        assert len(ops.uploaded_parts) == 2
        assert ops.uploaded_parts[0] == ("[part1]file.bin", b"hello ")
        assert ops.uploaded_parts[1] == ("[part2]file.bin", b"world")
        ops.commit_streamed_upload.assert_awaited_once()

    async def test_small_file_uploads_as_single_part_at_close(self, ops):
        handle = self._handle(ops, streaming_part_size=1024)
        await handle.write(0, b"hello world")
        await handle.close()

        assert len(ops.uploaded_parts) == 1
        assert ops.uploaded_parts[0] == ("[part1]file.bin", b"hello world")
        ops.commit_streamed_upload.assert_awaited_once()

    async def test_close_without_any_write_uploads_nothing(self, ops):
        handle = self._handle(ops)
        await handle.close()

        ops.upload_part.assert_not_awaited()
        ops.commit_streamed_upload.assert_not_awaited()

    async def test_abort_deletes_uploaded_parts(self, ops):
        handle = self._handle(ops, streaming_part_size=4)
        await handle.write(0, b"hello ")  # flushes one 4-byte part
        await handle.abort()

        ops.delete_uploaded_parts.assert_awaited_once()
        ops.commit_streamed_upload.assert_not_awaited()

    async def test_abort_without_uploaded_parts_is_harmless(self, ops):
        handle = self._handle(ops, streaming_part_size=1024)
        await handle.write(0, b"partial")
        await handle.abort()

        ops.delete_uploaded_parts.assert_not_awaited()

    async def test_close_after_abort_does_not_commit(self, ops):
        handle = self._handle(ops, streaming_part_size=4)
        await handle.write(0, b"hello ")
        await handle.abort()
        await handle.close()

        ops.commit_streamed_upload.assert_not_awaited()

    async def test_write_after_close_is_rejected(self, ops):
        handle = self._handle(ops)
        await handle.close()

        with pytest.raises(ValueError):
            await handle.write(0, b"late")

    async def test_non_sequential_write_before_any_part_uses_spool(self, ops):
        """Out-of-order write when nothing has been uploaded yet falls back
        to the spool without downloading anything back."""
        handle = self._handle(ops, streaming_part_size=1024)
        await handle.write(6, b"world")
        await handle.write(0, b"hello ")
        await handle.close()

        ops.upload_part.assert_not_awaited()
        ops.download_parts_back.assert_not_awaited()
        ops.upload_from_stream.assert_awaited_once()
        assert ops.uploaded["/file.bin"] == b"hello world"

    async def test_non_sequential_write_after_upload_falls_back_to_spool(
        self, ops
    ):
        """Non-sequential write after parts were pushed: delete them,
        download them back, and continue in spool mode."""
        handle = self._handle(ops, streaming_part_size=4)
        await handle.write(0, b"abcdef")  # flushes one 4-byte part: "abcd"
        # Non-sequential write at offset 0
        await handle.write(0, b"XY")
        await handle.close()

        ops.delete_uploaded_parts.assert_awaited_once()
        ops.download_parts_back.assert_awaited_once()
        ops.upload_from_stream.assert_awaited_once()
        # Spool has the downloaded part + buffer + the overwrite
        assert ops.uploaded["/file.bin"][:2] == b"XY"

    async def test_sparse_writes_are_zero_filled_via_spool(self, ops):
        handle = self._handle(ops, streaming_part_size=1024)
        await handle.write(4, b"tail")
        await handle.close()

        ops.upload_part.assert_not_awaited()
        assert ops.uploaded["/file.bin"] == b"\x00\x00\x00\x00tail"

    async def test_large_sequential_payload_flushes_multiple_parts(self, ops):
        payload = bytes(range(256)) * 512  # 128 KiB
        handle = self._handle(ops, streaming_part_size=4096, spool_max_bytes=4096)
        await handle.write(0, payload)
        await handle.close()

        assert len(ops.uploaded_parts) >= 2
        ops.commit_streamed_upload.assert_awaited_once()
