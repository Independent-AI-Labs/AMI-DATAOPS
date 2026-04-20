"""Unit tests for ami.dataops.intake.validation."""

from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from ami.dataops.intake import validation as v


class TestValidationRejected:
    def test_carries_reason_code_and_detail(self) -> None:
        exc = v.ValidationRejected("ext_not_allowed", "boom")
        assert exc.reason_code == "ext_not_allowed"
        assert exc.detail == "boom"
        assert "ext_not_allowed: boom" in str(exc)


class TestValidateExtension:
    @pytest.mark.parametrize(
        "path",
        ["a.log", "nested/dir/app.log", "UPPER.LOG"],
    )
    def test_accepts_allowlisted(self, path: str) -> None:
        v.validate_extension(path)

    @pytest.mark.parametrize(
        "path",
        [
            "script.sh",
            "bin.exe",
            "code.py",
            "noext",
            "a.pyc",
            "b.txt",
            "c.json",
            "d.ndjson",
            "e.md",
            "f.csv",
            "g.tsv",
            "h.yaml",
            "i.yml",
        ],
    )
    def test_rejects_non_allowlisted(self, path: str) -> None:
        with pytest.raises(v.ValidationRejected) as exc:
            v.validate_extension(path)
        assert exc.value.reason_code == "ext_not_allowed"


class TestProbeTextContent:
    def test_accepts_pure_text(self, tmp_path: Path) -> None:
        target = tmp_path / "ok.log"
        target.write_text("hello world\nline two\n")
        v.probe_text_content(target)

    def test_rejects_nul_in_first_bytes(self, tmp_path: Path) -> None:
        target = tmp_path / "binary.log"
        target.write_bytes(b"ok\x00text")
        with pytest.raises(v.ValidationRejected) as exc:
            v.probe_text_content(target)
        assert exc.value.reason_code == "not_text"

    def test_nul_past_probe_window_is_accepted(self, tmp_path: Path) -> None:
        target = tmp_path / "long.log"
        target.write_bytes(b"a" * 10_000 + b"\x00" + b"b" * 10)
        v.probe_text_content(target, probe_bytes=5000)

    def test_empty_file_is_text(self, tmp_path: Path) -> None:
        target = tmp_path / "empty.log"
        target.write_bytes(b"")
        v.probe_text_content(target)


class TestSizeValidators:
    def test_file_size_at_limit_accepted(self) -> None:
        v.validate_file_size(100, 100)

    def test_file_size_over_limit_rejected(self) -> None:
        with pytest.raises(v.ValidationRejected) as exc:
            v.validate_file_size(101, 100)
        assert exc.value.reason_code == "file_too_large"

    def test_bundle_aggregate_over_limit_rejected(self) -> None:
        with pytest.raises(v.ValidationRejected) as exc:
            v.validate_bundle_aggregate(501, 500)
        assert exc.value.reason_code == "bundle_too_large"

    def test_file_count_over_limit_rejected(self) -> None:
        with pytest.raises(v.ValidationRejected) as exc:
            v.validate_file_count(11, 10)
        assert exc.value.reason_code == "too_many_files"

    def test_aggregate_at_limit_accepted(self) -> None:
        v.validate_bundle_aggregate(500, 500)

    def test_count_at_limit_accepted(self) -> None:
        v.validate_file_count(10, 10)


class TestSha256:
    def test_compute_matches_hashlib(self, tmp_path: Path) -> None:
        target = tmp_path / "f.log"
        payload = b"deterministic payload"
        target.write_bytes(payload)
        assert v.compute_sha256(target) == hashlib.sha256(payload).hexdigest()

    def test_compute_streams_large_files(self, tmp_path: Path) -> None:
        target = tmp_path / "big.log"
        payload = b"chunk" * 200_000
        target.write_bytes(payload)
        expected = hashlib.sha256(payload).hexdigest()
        assert v.compute_sha256(target, chunk_bytes=1024) == expected

    def test_verify_hash_accepts_match(self, tmp_path: Path) -> None:
        target = tmp_path / "f.log"
        target.write_bytes(b"x")
        v.verify_hash(target, hashlib.sha256(b"x").hexdigest())

    def test_verify_hash_rejects_mismatch(self, tmp_path: Path) -> None:
        target = tmp_path / "f.log"
        target.write_bytes(b"x")
        with pytest.raises(v.ValidationRejected) as exc:
            v.verify_hash(target, "0" * 64)
        assert exc.value.reason_code == "hash_mismatch"


def _make_tar(entries: list[tuple[str, bytes]], *, compresslevel: int = 1) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=compresslevel) as tar:
        for name, payload in entries:
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


def _make_tar_with_symlink(dest: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="evil.log")
        info.type = tarfile.SYMTYPE
        info.linkname = dest
        tar.addfile(info)
    return buf.getvalue()


def _make_tar_with_traversal() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="../escape.log")
        info.size = 4
        tar.addfile(info, io.BytesIO(b"boom"))
    return buf.getvalue()


def _make_tar_with_setuid() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="privileged.log")
        info.size = 4
        info.mode = 0o4755
        tar.addfile(info, io.BytesIO(b"boom"))
    return buf.getvalue()


class TestExtractBundleStream:
    def test_happy_path(self, tmp_path: Path) -> None:
        payload = _make_tar([("a.log", b"alpha\n"), ("nested/b.log", b"beta\n")])
        staging = tmp_path / "stage"
        files = v.extract_bundle_stream(io.BytesIO(payload), staging)
        names = {p.relative_to(staging).as_posix() for p in files}
        assert names == {"a.log", "nested/b.log"}
        assert (staging / "a.log").read_bytes() == b"alpha\n"

    def test_rejects_disallowed_extension(self, tmp_path: Path) -> None:
        payload = _make_tar([("a.exe", b"bad")])
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(io.BytesIO(payload), tmp_path / "stage")
        assert exc.value.reason_code == "ext_not_allowed"

    def test_rejects_symlink_member(self, tmp_path: Path) -> None:
        payload = _make_tar_with_symlink("/etc/passwd")
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(io.BytesIO(payload), tmp_path / "stage")
        assert exc.value.reason_code == "path_unsafe"

    def test_rejects_path_traversal_member(self, tmp_path: Path) -> None:
        payload = _make_tar_with_traversal()
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(io.BytesIO(payload), tmp_path / "stage")
        assert exc.value.reason_code == "path_unsafe"

    def test_absolute_path_is_rewritten_relative(self, tmp_path: Path) -> None:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            info = tarfile.TarInfo(name="/etc/ok.log")
            info.size = 4
            tar.addfile(info, io.BytesIO(b"text"))
        staging = tmp_path / "stage"
        files = v.extract_bundle_stream(io.BytesIO(buf.getvalue()), staging)
        assert files == [staging / "etc" / "ok.log"]

    def test_setuid_bit_is_stripped_by_data_filter(self, tmp_path: Path) -> None:
        payload = _make_tar_with_setuid()
        staging = tmp_path / "stage"
        files = v.extract_bundle_stream(io.BytesIO(payload), staging)
        assert files == [staging / "privileged.log"]
        mode = (staging / "privileged.log").stat().st_mode & 0o7777
        assert mode & 0o4000 == 0, f"setuid bit still set: {mode:o}"

    def test_rejects_file_over_per_file_cap(self, tmp_path: Path) -> None:
        payload = _make_tar([("a.log", b"x" * 2048)])
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(
                io.BytesIO(payload),
                tmp_path / "stage",
                v.ExtractionLimits(max_file_bytes=1024),
            )
        assert exc.value.reason_code == "file_too_large"

    def test_rejects_aggregate_over_bundle_cap(self, tmp_path: Path) -> None:
        payload = _make_tar([("a.log", b"x" * 800), ("b.log", b"y" * 800)])
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(
                io.BytesIO(payload),
                tmp_path / "stage",
                v.ExtractionLimits(max_bundle_bytes=1000),
            )
        assert exc.value.reason_code == "bundle_too_large"

    def test_rejects_over_file_count_cap(self, tmp_path: Path) -> None:
        payload = _make_tar([(f"f{idx}.log", b"x") for idx in range(5)])
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(
                io.BytesIO(payload),
                tmp_path / "stage",
                v.ExtractionLimits(max_files=3),
            )
        assert exc.value.reason_code == "too_many_files"

    def test_post_extract_null_byte_probe_rejects(self, tmp_path: Path) -> None:
        payload = _make_tar([("a.log", b"ok\x00bad")])
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(io.BytesIO(payload), tmp_path / "stage")
        assert exc.value.reason_code == "not_text"
