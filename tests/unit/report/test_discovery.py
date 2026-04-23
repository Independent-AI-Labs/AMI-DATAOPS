"""Unit tests for ami.dataops.report.discovery."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.discovery import candidates_for, find_scope_candidates
from ami.dataops.report.scanner import SKIP_DIRS

_EXPECTED_TOTAL_FILES = 3
_EXPECTED_CANDIDATE_COUNT = 2  # root + nested/
_EXPECTED_SUFFIX_WIDENED_COUNT = 2  # .log + .txt


def _write_log(path: Path, body: str = "log line\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


class TestFindScopeCandidates:
    def test_root_with_matches_appears_first_with_total_count(
        self, tmp_path: Path
    ) -> None:
        _write_log(tmp_path / "a.log")
        _write_log(tmp_path / "nested" / "b.log")
        _write_log(tmp_path / "nested" / "c.log")
        result = find_scope_candidates(tmp_path)
        assert len(result) == _EXPECTED_CANDIDATE_COUNT
        assert result[0].absolute_path == tmp_path.resolve()
        assert result[0].file_count == _EXPECTED_TOTAL_FILES
        assert result[1].absolute_path == (tmp_path / "nested").resolve()

    def test_skip_dirs_are_pruned(self, tmp_path: Path) -> None:
        """Skipped dirs never appear as candidates nor inflate the root count."""
        _write_log(tmp_path / "kept.log")
        skipped_dirs = [".git", ".venv", "projects", "__pycache__", "node_modules"]
        for skipped in skipped_dirs:
            _write_log(tmp_path / skipped / "hidden.log")
        result = find_scope_candidates(tmp_path)
        root_path = tmp_path.resolve()
        # Only the root itself is reported — none of the junk subdirs.
        reported = [c.absolute_path for c in result]
        assert reported == [root_path]
        # Count reflects only the kept file, not the five hidden ones.
        assert result[0].file_count == 1
        # Every SCOPE_SKIP_DIR we created must be absent from the result.
        for skipped in skipped_dirs:
            assert (tmp_path / skipped).resolve() not in reported
        assert SKIP_DIRS.issuperset(skipped_dirs)

    def test_suffix_override_widens_match(self, tmp_path: Path) -> None:
        _write_log(tmp_path / "a.log")
        _write_log(tmp_path / "b.txt", "plain text\n")
        default = find_scope_candidates(tmp_path)
        assert default[0].file_count == 1
        widened = find_scope_candidates(tmp_path, allowed_suffixes=(".log", ".txt"))
        assert widened[0].file_count == _EXPECTED_SUFFIX_WIDENED_COUNT


class TestCandidatesFor:
    def test_dedupes_across_roots(self, tmp_path: Path) -> None:
        _write_log(tmp_path / "a.log")
        same = tmp_path
        result = candidates_for(tmp_path, [same])
        paths = [c.absolute_path for c in result]
        assert len(paths) == len(set(paths))

    def test_combines_ami_root_and_extras(self, tmp_path: Path) -> None:
        ami_root = tmp_path / "ami"
        extra = tmp_path / "extra"
        _write_log(ami_root / "a.log")
        _write_log(extra / "b.log")
        result = candidates_for(ami_root, [extra])
        paths = {c.absolute_path for c in result}
        assert ami_root.resolve() in paths
        assert extra.resolve() in paths

    def test_missing_ami_root_is_ignored(self, tmp_path: Path) -> None:
        extra = tmp_path / "extra"
        _write_log(extra / "a.log")
        result = candidates_for(None, [extra])
        assert any(c.absolute_path == extra.resolve() for c in result)
