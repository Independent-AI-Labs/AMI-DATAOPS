# Backup Restore Module

This module provides a modular, testable architecture for restoring backups from Google Drive or local files.

## Architecture

The backup restore functionality has been restructured into several focused modules:

### Core Components

- `common/auth.py`: Authentication manager with support for multiple auth methods (impersonation, key, OAuth, user app)
- `core/config.py`: Configuration management extending the base backup config
- `restore/extractor.py`: Archive extraction with intelligent file handling
- `restore/drive_client.py`: Google Drive API interactions
- `restore/local_client.py`: Local file operations
- `restore/service.py`: Main business logic orchestration
- `restore/selector.py`: Interactive backup selection UI
- `restore/cli.py`: Command line interface
- `restore/main.py`: Main entry point with dependency injection

### Key Improvements

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Testability**: Components can be tested in isolation with dependency injection
3. **Maintainability**: Changes in one area don't affect others
4. **Reusability**: Components can be used in different contexts
5. **Error Handling**: Proper exception handling at each layer

## Usage

The new script maintains all the same command-line functionality as the original:

```bash
# Restore from specific Google Drive file ID
python -m scripts.backup.restore.main --file-id 1aBcDeFgHiJkLmNoPqRsTuVwXyZ

# Restore the latest local backup
python -m scripts.backup.restore.main --latest-local

# Restore from specific local file
python -m scripts.backup.restore.main --local-path /path/to/backup.tar.zst

# Go back N revisions in Google Drive backups (new functionality)
python -m scripts.backup.restore.main --revision 2 --dest /tmp/restore

# Interactive selection of backup
python -m scripts.backup.restore.main --interactive

# Restore with custom location
python -m scripts.backup.restore.main --latest-local --restore-path /opt/ami-restored
```

## Migration Notes

The new architecture preserves all functionality from the original monolithic script while adding:

- Better error handling
- Improved test coverage
- Modularity and separation of concerns
- The new `--revision` functionality for going back N revisions like Git

The old script is preserved as `backup_restore_old.py` for reference but the new modular approach should be used going forward.