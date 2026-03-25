"""Unit tests for the DriveRestoreClient service (restore/drive_client.py)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.restore.drive_client import DriveRestoreClient

# Test constants
EXPECTED_BACKUP_FILE_COUNT = 2


class TestDriveRestoreClient:
    """Unit tests for the DriveRestoreClient class."""

    def test_initialization(self):
        """Test that DriveRestoreClient initializes with auth manager."""
        mock_auth_manager = MagicMock()
        client = DriveRestoreClient(mock_auth_manager)

        assert client.auth_manager == mock_auth_manager
        assert client._service is None

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_get_service_creates_google_service(self, mock_build):
        """Test that _get_service creates the Google Drive service."""
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        client = DriveRestoreClient(mock_auth_manager)

        # Call the async method
        service = await client._get_service()

        assert service == mock_service
        mock_build.assert_called_once_with("drive", "v3", credentials=mock_credentials)
        # Verify it's cached
        assert client._service == mock_service

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_list_backup_files_success(self, mock_build):
        """Test successful listing of backup files."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_files_resource = MagicMock()
        mock_service.files.return_value = mock_files_resource
        mock_list_request = MagicMock()
        mock_files_resource.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {
            "files": [
                {
                    "id": "file1_id",
                    "name": "ami-orchestrator-backup_20231201_120000.tar.zst",
                    "modifiedTime": "2023-12-01T12:00:00Z",
                    "size": "1048576",
                },
                {
                    "id": "file2_id",
                    "name": "ami-orchestrator-backup_20231130_120000.tar.zst",
                    "modifiedTime": "2023-11-30T12:00:00Z",
                    "size": "2097152",
                },
            ]
        }

        mock_build.return_value = mock_service

        # Create client and config
        client = DriveRestoreClient(mock_auth_manager)
        config = MagicMock()
        config.folder_id = "test_folder_id"

        # Run the method
        result = await client.list_backup_files(config)

        # Verify results
        assert len(result) == EXPECTED_BACKUP_FILE_COUNT
        assert result[0]["id"] == "file1_id"

        # Verify the call was made correctly
        mock_files_resource.list.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_list_backup_files_no_files(self, mock_build):
        """Test listing backup files when no files exist."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_files_resource = MagicMock()
        mock_service.files.return_value = mock_files_resource
        mock_list_request = MagicMock()
        mock_files_resource.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {"files": []}  # No files

        mock_build.return_value = mock_service

        # Create client and config
        client = DriveRestoreClient(mock_auth_manager)
        config = MagicMock()
        config.folder_id = None  # No specific folder

        # Run the method
        result = await client.list_backup_files(config)

        # Verify results
        assert result == []  # Empty list

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.MediaIoBaseDownload")
    @patch("ami.dataops.backup.restore.drive_client.build")
    @patch("builtins.open")
    async def test_download_file_success(
        self, mock_open_func, mock_build, mock_downloader_class
    ):
        """Test successful download of a file."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_files_resource = MagicMock()
        mock_service.files.return_value = mock_files_resource

        # Mock get file metadata request
        mock_get_request = MagicMock()
        mock_files_resource.get.return_value = mock_get_request
        mock_get_request.execute.return_value = {
            "name": "test_backup.tar.zst",
            "size": "1048576",
        }

        mock_build.return_value = mock_service

        # Mock MediaIoBaseDownload
        mock_downloader_instance = MagicMock()
        mock_status = MagicMock()
        mock_status.progress.return_value = 1.0
        mock_downloader_instance.next_chunk.return_value = (
            mock_status,
            True,
        )  # 100%, done
        mock_downloader_class.return_value = mock_downloader_instance

        # Mock file context manager
        mock_file = MagicMock()
        mock_open_func.return_value.__enter__.return_value = mock_file
        mock_open_func.return_value.__exit__ = MagicMock(return_value=False)

        # Create client and config
        client = DriveRestoreClient(mock_auth_manager)
        config = MagicMock()
        destination = Path("/tmp/test_backup.tar.zst")

        # Mock destination parent mkdir
        with patch.object(Path, "mkdir"):
            # Run the method
            result = await client.download_file("test_file_id", destination, config)

        # Verify results
        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_get_file_metadata_success(self, mock_build):
        """Test getting file metadata successfully."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_files_resource = MagicMock()
        mock_service.files.return_value = mock_files_resource

        # Mock get file metadata request
        mock_get_request = MagicMock()
        mock_files_resource.get.return_value = mock_get_request
        mock_get_request.execute.return_value = {
            "id": "test_file_id",
            "name": "test_backup.tar.zst",
            "size": "1048576",
            "modifiedTime": "2023-12-01T12:00:00Z",
        }

        mock_build.return_value = mock_service

        # Create client
        client = DriveRestoreClient(mock_auth_manager)

        # Run the method
        result = await client.get_file_metadata("test_file_id")

        # Verify results - the result is a DriveFileMetadata TypedDict
        assert result is not None
        assert result["id"] == "test_file_id"
        assert result["name"] == "test_backup.tar.zst"

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_verify_backup_exists_success(self, mock_build):
        """Test verifying backup exists when it does."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_files_resource = MagicMock()
        mock_service.files.return_value = mock_files_resource

        # Mock get file metadata request to return metadata (exists)
        mock_get_request = MagicMock()
        mock_files_resource.get.return_value = mock_get_request
        mock_get_request.execute.return_value = {
            "id": "test_file_id",
            "name": "test_backup.tar.zst",
        }

        mock_build.return_value = mock_service

        # Create client
        client = DriveRestoreClient(mock_auth_manager)

        # Run the method
        result = await client.verify_backup_exists("test_file_id")

        # Verify results
        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_list_backup_files_exception(self, mock_build):
        """Test list returns empty on exception."""
        mock_auth = MagicMock()
        mock_auth.get_credentials.return_value = MagicMock()
        svc = MagicMock()
        svc.files.return_value.list.return_value.execute.side_effect = Exception(
            "API error"
        )
        mock_build.return_value = svc
        client = DriveRestoreClient(mock_auth)
        result = await client.list_backup_files(MagicMock(folder_id=None))
        assert result == []

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_get_metadata_exception(self, mock_build):
        """Test get_file_metadata returns None on error."""
        mock_auth = MagicMock()
        mock_auth.get_credentials.return_value = MagicMock()
        svc = MagicMock()
        svc.files.return_value.get.return_value.execute.side_effect = Exception(
            "Not found"
        )
        mock_build.return_value = svc
        client = DriveRestoreClient(mock_auth)
        result = await client.get_file_metadata("bad_id")
        assert result is None

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.drive_client.build")
    async def test_verify_exists_false_on_error(self, mock_build):
        """Test verify returns False on exception."""
        mock_auth = MagicMock()
        mock_auth.get_credentials.return_value = MagicMock()
        svc = MagicMock()
        svc.files.return_value.get.return_value.execute.side_effect = Exception("err")
        mock_build.return_value = svc
        client = DriveRestoreClient(mock_auth)
        result = await client.verify_backup_exists("bad")
        assert result is False
