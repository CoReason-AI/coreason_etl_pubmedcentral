# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import io
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, ConnectionError

from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType
from coreason_etl_pubmedcentral.utils.logger import logger


@pytest.fixture  # type: ignore[misc]
def log_capture() -> Any:
    """Captures loguru logs."""
    logs = []
    handler_id = logger.add(lambda msg: logs.append(msg), format="{message}", level="INFO")
    yield logs
    logger.remove(handler_id)


@pytest.fixture  # type: ignore[misc]
def source_manager() -> Generator[SourceManager, None, None]:
    with patch("boto3.client"):
        sm = SourceManager()
        # Mock the s3 client instance
        sm._s3_client = MagicMock()
        yield sm


def test_s3_success(source_manager: SourceManager) -> None:
    # Setup
    mock_body = io.BytesIO(b"file content")
    source_manager._s3_client.get_object.return_value = {"Body": mock_body}

    # Execute
    data = source_manager.get_file("path/to/file.xml")

    # Verify
    assert data == b"file content"
    assert source_manager._current_source == SourceType.S3
    assert source_manager._s3_consecutive_errors == 0
    source_manager._s3_client.get_object.assert_called_with(Bucket="pmc-oa-opendata", Key="path/to/file.xml")


def test_s3_client_error_no_failover_count(source_manager: SourceManager) -> None:
    # Setup: ClientError (e.g., 404) should NOT increment failover counter
    error = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")
    source_manager._s3_client.get_object.side_effect = error

    with pytest.raises(ClientError):
        source_manager.get_file("path/fail.xml")

    # Verify counter did NOT increment
    assert source_manager._s3_consecutive_errors == 0
    assert source_manager._current_source == SourceType.S3


def test_s3_client_error_resets_failover_count(source_manager: SourceManager) -> None:
    """
    Verifies that a ClientError (e.g. 404) resets the connection error counter,
    as it implies successful connectivity.
    """
    conn_error = ConnectionError(error=Exception("Connection Refused"))
    client_error = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")

    # Sequence: Connection Error -> Client Error (404) -> Connection Error
    source_manager._s3_client.get_object.side_effect = [conn_error, client_error, conn_error]

    # 1. Connection Error
    with pytest.raises(ConnectionError):
        source_manager.get_file("f1")
    assert source_manager._s3_consecutive_errors == 1

    # 2. Client Error (Should reset counter)
    with pytest.raises(ClientError):
        source_manager.get_file("f2")
    assert source_manager._s3_consecutive_errors == 0

    # 3. Connection Error (Should be 1 again, not 2)
    with pytest.raises(ConnectionError):
        source_manager.get_file("f3")
    assert source_manager._s3_consecutive_errors == 1


def test_s3_connection_error_failover_count(source_manager: SourceManager) -> None:
    # Setup: ConnectionError SHOULD increment failover counter
    error = ConnectionError(error=Exception("Connection Refused"))
    mock_body = io.BytesIO(b"file content")
    source_manager._s3_client.get_object.side_effect = [error, {"Body": mock_body}]

    # Execute 1: Should raise exception and increment counter
    with pytest.raises(ConnectionError):
        source_manager.get_file("path/fail.xml")

    assert source_manager._s3_consecutive_errors == 1
    assert source_manager._current_source == SourceType.S3

    # Execute 2: Success -> Reset counter
    data = source_manager.get_file("path/success.xml")
    assert data == b"file content"
    assert source_manager._s3_consecutive_errors == 0


def test_s3_failover(source_manager: SourceManager, log_capture: list[str]) -> None:
    # Setup: 3 consecutive Connection errors
    error = ConnectionError(error=Exception("Connection Refused"))
    source_manager._s3_client.get_object.side_effect = [error, error, error]

    # Mock FTP to verify it gets called immediately on the 3rd failure
    with patch("ftplib.FTP") as mock_ftp_cls:
        mock_ftp = mock_ftp_cls.return_value

        # Mock retrbinary to write data to the callback
        def side_effect_retrbinary(cmd: str, callback: Any) -> None:
            callback(b"ftp content")

        mock_ftp.retrbinary.side_effect = side_effect_retrbinary

        # 1st Failure
        with pytest.raises(ConnectionError):
            source_manager.get_file("f1")
        assert source_manager._s3_consecutive_errors == 1

        # 2nd Failure
        with pytest.raises(ConnectionError):
            source_manager.get_file("f2")
        assert source_manager._s3_consecutive_errors == 2

        # 3rd Failure -> Trigger Failover -> Fetch from FTP immediately
        data = source_manager.get_file("f3")

        assert data == b"ftp content"
        assert source_manager._current_source == SourceType.FTP
        # Verify FTP call
        mock_ftp_cls.assert_called_with("ftp.ncbi.nlm.nih.gov")
        mock_ftp.retrbinary.assert_called()
        # Verify full path construction
        args, _ = mock_ftp.retrbinary.call_args
        assert args[0] == "RETR /pub/pmc/f3"

        # Verify Log Message (Strict Format)
        assert any("FailoverEvent — S3 unreachable. Switched to FTP" in msg and "f3" in msg for msg in log_capture)


def test_ftp_direct_usage_after_failover(source_manager: SourceManager) -> None:
    # Manually set state to FTP
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        mock_ftp = mock_ftp_cls.return_value

        def side_effect_retrbinary(cmd: str, callback: Any) -> None:
            callback(b"ftp direct")

        mock_ftp.retrbinary.side_effect = side_effect_retrbinary

        data = source_manager.get_file("some/file.xml")

        assert data == b"ftp direct"
        mock_ftp.login.assert_called()


def test_ftp_reconnect_on_noop_failure(source_manager: SourceManager) -> None:
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        # Initial mock (will fail voidcmd)
        mock_ftp_initial = MagicMock()
        mock_ftp_initial.voidcmd.side_effect = EOFError("Connection closed")

        # New mock (will succeed)
        mock_ftp_new = MagicMock()

        def side_effect_retrbinary(cmd: str, callback: Any) -> None:
            callback(b"reconnected data")

        mock_ftp_new.retrbinary.side_effect = side_effect_retrbinary

        # We start with _ftp populated with the initial mock
        source_manager._ftp = mock_ftp_initial

        # When _ensure_ftp_connection runs, it detects failure on initial, closes it,
        # then calls ftplib.FTP(...) to create new one.
        # We want ftplib.FTP constructor to return mock_ftp_new
        mock_ftp_cls.return_value = mock_ftp_new

        data = source_manager.get_file("file")

        assert data == b"reconnected data"
        # Verify initial failed check
        mock_ftp_initial.voidcmd.assert_called_with("NOOP")
        # Verify new connection made
        mock_ftp_cls.assert_called_with("ftp.ncbi.nlm.nih.gov")
        mock_ftp_new.login.assert_called()


def test_ftp_retry_on_fetch_failure(source_manager: SourceManager) -> None:
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        # Initial mock: voidcmd passes, but retrbinary fails
        mock_ftp_initial = MagicMock()
        mock_ftp_initial.voidcmd.return_value = "OK"
        mock_ftp_initial.retrbinary.side_effect = EOFError("Stream lost")

        # New mock: succeeds
        mock_ftp_new = MagicMock()

        def side_effect_retrbinary_success(cmd: str, callback: Any) -> None:
            callback(b"retry data")

        mock_ftp_new.retrbinary.side_effect = side_effect_retrbinary_success

        source_manager._ftp = mock_ftp_initial

        # Constructor should return new mock when called
        mock_ftp_cls.return_value = mock_ftp_new

        data = source_manager.get_file("file")

        assert data == b"retry data"
        # Verify retry logic:
        # 1. Calls retrbinary on initial -> fails
        # 2. Closes initial
        # 3. Connects new
        # 4. Calls retrbinary on new -> succeeds
        mock_ftp_cls.assert_called()


def test_ftp_flakey_connection_loop(source_manager: SourceManager) -> None:
    """
    Simulates a very flakey FTP connection:
    Connect -> Fail Fetch -> Reconnect -> Fail Fetch -> Reconnect -> Success
    """
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        # Mock 1: Initial connection, fails fetch
        m1 = MagicMock()
        m1.retrbinary.side_effect = EOFError("Fail 1")

        # Mock 2: Reconnection 1, fails fetch
        m2 = MagicMock()
        m2.retrbinary.side_effect = EOFError("Fail 2")

        # Mock 3: Reconnection 2, succeeds
        m3 = MagicMock()
        m3.retrbinary.side_effect = lambda cmd, cb: cb(b"success")

        # Cycle through mocks
        # _ensure_ftp_connection calls FTP() when it needs a NEW connection.
        # It DOES NOT call FTP() if _ftp is already set (unless checking NOOP fails).

        # We start with source_manager._ftp = m1 manually.
        # Flow in get_file:
        # 1. _ensure_ftp_connection: checks m1.voidcmd("NOOP"). We assume it passes?
        #    If m1 is "new", voidcmd might not be called if we don't mock it to fail?

        #    So m1 needs to pass voidcmd if we want to use it for retrbinary.
        m1.voidcmd.return_value = "OK"

        # 2. m1.retrbinary fails with "Fail 1".
        # 3. catch block:
        #    _close_ftp() -> m1 closed. self._ftp = None.
        #    _ensure_ftp_connection() -> self._ftp is None -> calls ftplib.FTP().
        #    This call consumes the FIRST element of side_effect.

        # So if side_effect = [m1, m2, m3], it returns m1 AGAIN.
        # Then m1.retrbinary is called again -> "Fail 1" again.

        # FIX: The side_effect should provide the *next* connections: [m2, m3].
        mock_ftp_cls.side_effect = [m2, m3]

        # Let's start with _ftp = m1
        source_manager._ftp = m1
        # The first call to get_file will try m1.retrbinary -> raises EOFError
        # Then catches error -> closes -> ensures connection (calls FTP() -> gets m2) -> retries -> raises EOFError
        # So it fails after ONE retry.

        with pytest.raises(EOFError) as exc:
            source_manager.get_file("file")
        assert str(exc.value) == "Fail 2"

        # Verify m1 closed
        # The logic calls quit() first. If quit fails, close().
        # m1.quit() should have been called.
        assert m1.quit.called or m1.close.called

        # Now _ftp is m2.
        # Next call to get_file:
        # m2 is set. Checks NOOP.
        # If we want to simulate m2 failing fetch again?
        # m2 is already dead from previous call? No, code doesn't close on the *retry* failure inside
        # `get_file` catch block.
        # Wait, looking at code:
        # try: retrbinary except: close, ensure(new), retry.
        # If retry fails, it raises. The new connection (m2) is left open in self._ftp.

        # So next call:
        # _ensure_ftp_connection calls m2.voidcmd("NOOP").
        # If we want that to pass?
        m2.voidcmd.return_value = "OK"
        # Then it calls m2.retrbinary -> EOFError("Fail 2") again?
        # We need to configure side effects carefully.

        # Let's adjust scenario:
        # 1. get_file -> m1 fails -> reconnect m2 -> m2 succeeds.
        # This covers "Retry on fetch failure". Already covered by test_ftp_retry_on_fetch_failure.

        # Scenario: Flakey connection.
        # m1 fails. m2 fails. Exception raised. User retries (dlt).
        # Next call -> m2 NOOP fails -> m3 connects -> m3 succeeds.

        # Reset mocks
        m1.reset_mock()
        m2.reset_mock()
        m3.reset_mock()

        # Setup side effects again
        mock_ftp_cls.side_effect = [m2, m3]  # m1 is already "set"

        # m1 fails fetch
        m1.retrbinary.side_effect = EOFError("Fail 1")

        # m2 fails retry fetch
        m2.retrbinary.side_effect = EOFError("Fail 2")

        # m2 NOOP fails (for next call)
        m2.voidcmd.side_effect = EOFError("NOOP Fail")

        # m3 succeeds fetch
        m3.retrbinary.side_effect = lambda cmd, cb: cb(b"success")

        source_manager._ftp = m1

    # Call 1: Succeeds (Improved robustness handles m2 failure via internal _ensure check)
    # Flow:
    # Attempt 1 (m1) -> Fail
    # before_sleep -> reconnect (m2)
    # Attempt 2 -> _ensure checks m2.voidcmd -> Fail -> reconnect (m3) -> fetch (m3) -> Success
        data = source_manager.get_file("file")
        assert data == b"success"

        # Verify sequence
        # m1 used then closed
        # m2 created, used, then closed (after NOOP fail)
        # m3 created, used
        assert m3.retrbinary.called
        m3.login.assert_called()


def test_close(source_manager: SourceManager) -> None:
    mock_ftp = MagicMock()
    source_manager._ftp = mock_ftp
    source_manager.close()
    mock_ftp.quit.assert_called()
    assert source_manager._ftp is None


def test_ftp_connection_failure(source_manager: SourceManager) -> None:
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        # Simulate connection failure
        mock_ftp_cls.side_effect = OSError("Host unreachable")

        with pytest.raises(OSError):
            source_manager.get_file("file")

        assert source_manager._ftp is None


def test_ftp_reconnect_failure(source_manager: SourceManager) -> None:
    """Test when reconnecting after a fetch failure also fails."""
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        # 1. Initial connection exists
        mock_ftp_initial = MagicMock()
        mock_ftp_initial.retrbinary.side_effect = EOFError("Lost")
        source_manager._ftp = mock_ftp_initial

        # 2. Reconnection fails (returns None or raises?)
        # _ensure_ftp_connection will try to create new FTP.
        # Let's make it raise exception.
        mock_ftp_cls.side_effect = OSError("Cannot reconnect")

        # We expect the ORIGINAL error (EOFError) or the NEW error (OSError)?
        # Code:
        # except ... as e:
        #   _close_ftp()
        #   _ensure_ftp_connection() -> Raises OSError
        # So we expect OSError.

        with pytest.raises(OSError):
            source_manager.get_file("file")


def test_ftp_reconnect_success_but_second_fetch_fails(source_manager: SourceManager) -> None:
    """Test when we reconnect successfully, but the retry fetch also fails."""
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        mock_ftp_initial = MagicMock()
        mock_ftp_initial.retrbinary.side_effect = EOFError("Lost 1")
        source_manager._ftp = mock_ftp_initial

        mock_ftp_new = MagicMock()
        mock_ftp_new.retrbinary.side_effect = EOFError("Lost 2")
        mock_ftp_cls.return_value = mock_ftp_new

        with pytest.raises(EOFError) as exc:
            source_manager.get_file("file")

        assert str(exc.value) == "Lost 2"


def test_ftp_close_exception(source_manager: SourceManager) -> None:
    """Verify exceptions during close are swallowed."""
    mock_ftp = MagicMock()
    mock_ftp.quit.side_effect = EOFError("Quit failed")
    mock_ftp.close.side_effect = OSError("Close failed")

    source_manager._ftp = mock_ftp

    # Should not raise
    source_manager.close()

    assert source_manager._ftp is None


def test_ensure_ftp_connection_failure_handling(source_manager: SourceManager) -> None:
    """Verify handling when _ensure_ftp_connection fails to set _ftp."""
    source_manager._current_source = SourceType.FTP

    # We need to simulate a case where FTP() succeeds but returns something that ends up being None?
    # Or strict exception raising.
    # The code:
    # try: self._ftp = ftplib.FTP(...)
    # except: self._ftp = None; raise
    # So if it fails, it raises.
    # But what if self._ftp is None after _ensure_ftp_connection returns?
    # This happens if it was None and logic was skipped?
    # No, it checks if self._ftp: ... else: connect.
    # So it either raises or sets self._ftp.

    # However, let's verify the line:
    # if not self._ftp: raise RuntimeError(...)
    # This is defensively reachable if _ensure_ftp_connection somehow returned without raising but _ftp is None.
    # We can mock _ensure_ftp_connection to do nothing.

    with patch.object(source_manager, "_ensure_ftp_connection", return_value=None):
        source_manager._ftp = None

        with pytest.raises(RuntimeError, match="FTP connection could not be established"):
            source_manager.get_file("file")


def test_ftp_reconnect_returns_none_silent_failure(source_manager: SourceManager) -> None:
    """
    Cover line 129: if not self._ftp: raise RuntimeError.
    This requires _ensure_ftp_connection to return successfully but _ftp to be None.
    """
    source_manager._current_source = SourceType.FTP
    mock_ftp = MagicMock()
    mock_ftp.retrbinary.side_effect = EOFError("Initial Failure")
    source_manager._ftp = mock_ftp

    # Mock _ensure_ftp_connection to do nothing (so _ftp becomes None after _close_ftp)
    # logic:
    # catch error -> _close_ftp() (sets _ftp=None) -> _ensure_ftp_connection() (mocked, does nothing)
    # -> check if not self._ftp -> raise RuntimeError

    with patch.object(source_manager, "_ensure_ftp_connection", return_value=None):
        with pytest.raises(RuntimeError, match="FTP connection could not be established"):
            source_manager.get_file("file")


def test_unknown_source_state(source_manager: SourceManager) -> None:
    """Cover the unreachable state error."""
    source_manager._current_source = "INVALID"  # type: ignore
    with pytest.raises(RuntimeError, match="Unknown source state"):
        source_manager.get_file("file")


# Complex Edge Cases


def test_s3_intermittent_failures_reset_counter(source_manager: SourceManager) -> None:
    """Verify that intermittent failures do not trigger failover if success occurs in between."""
    error = ConnectionError(error=Exception("Connection Refused"))
    mock_body = io.BytesIO(b"content")

    # Pattern: Fail, Fail, Success, Fail, Fail, Success
    source_manager._s3_client.get_object.side_effect = [
        error,
        error,
        {"Body": mock_body},
        error,
        error,
        {"Body": mock_body},
    ]

    # 1. Fail
    with pytest.raises(ConnectionError):
        source_manager.get_file("f1")
    assert source_manager._s3_consecutive_errors == 1

    # 2. Fail
    with pytest.raises(ConnectionError):
        source_manager.get_file("f2")
    assert source_manager._s3_consecutive_errors == 2

    # 3. Success -> Should reset
    source_manager.get_file("f3")
    assert source_manager._s3_consecutive_errors == 0

    # 4. Fail
    with pytest.raises(ConnectionError):
        source_manager.get_file("f4")
    assert source_manager._s3_consecutive_errors == 1
    assert source_manager._current_source == SourceType.S3


def test_failover_persistence_explicit(source_manager: SourceManager) -> None:
    """Verify that after failover, subsequent calls strictly use FTP."""
    error = ConnectionError(error=Exception("Connection Refused"))
    source_manager._s3_client.get_object.side_effect = [error, error, error]

    with patch("ftplib.FTP") as mock_ftp_cls:
        mock_ftp = mock_ftp_cls.return_value

        # Trigger failover
        with pytest.raises(ConnectionError):
            source_manager.get_file("f1")
        with pytest.raises(ConnectionError):
            source_manager.get_file("f2")

        # 3rd failure triggers switch and immediate FTP fetch
        source_manager.get_file("f3")
        assert source_manager._current_source == SourceType.FTP

        # Reset S3 mock to verify it's NOT called anymore
        source_manager._s3_client.get_object.reset_mock()

        # Subsequent calls
        for i in range(5):
            source_manager.get_file(f"next_{i}")

        source_manager._s3_client.get_object.assert_not_called()
        assert mock_ftp.retrbinary.call_count == 1 + 5  # 1 initial + 5 subsequent


def test_empty_file_handling(source_manager: SourceManager) -> None:
    """Verify handling of empty files from both sources."""
    # S3 Empty
    source_manager._s3_client.get_object.return_value = {"Body": io.BytesIO(b"")}
    assert source_manager.get_file("empty_s3.xml") == b""

    # FTP Empty
    source_manager._current_source = SourceType.FTP
    with patch("ftplib.FTP"):
        # retrbinary does nothing (writes nothing)
        assert source_manager.get_file("empty_ftp.xml") == b""


def test_ftp_timeout_during_transfer(source_manager: SourceManager) -> None:
    """Simulate a timeout during FTP transfer and verify retry."""
    source_manager._current_source = SourceType.FTP

    with patch("ftplib.FTP") as mock_ftp_cls:
        mock_ftp_initial = MagicMock()
        mock_ftp_initial.voidcmd.return_value = "OK"
        # Simulate TimeoutError (wrapped or raw)
        mock_ftp_initial.retrbinary.side_effect = TimeoutError("Socket timed out")

        mock_ftp_new = MagicMock()

        def side_effect_success(cmd: str, callback: Any) -> None:
            callback(b"recovered")

        mock_ftp_new.retrbinary.side_effect = side_effect_success

        source_manager._ftp = mock_ftp_initial
        mock_ftp_cls.return_value = mock_ftp_new

        data = source_manager.get_file("timeout_file")
        assert data == b"recovered"

        # Verify reconnection happened
        mock_ftp_cls.assert_called()
