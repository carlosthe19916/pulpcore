import os
from datetime import datetime, timezone

from aiohttp.test_utils import make_mocked_request
from aiohttp.web import FileResponse
from aiohttp.web_fileresponse import _FileResponseResult
from django.utils.http import http_date

from pulpcore.responses import PulpFileResponse


def test_pulp_file_response_preserves_handler_last_modified(tmp_path):
    """A handler-set Last-Modified is not overwritten by aiohttp's file-mtime assignment."""
    path = tmp_path / "artifact"
    path.write_text("data")
    ours = http_date(1_000_000_000)  # a fixed, non-mtime value
    response = PulpFileResponse(path, headers={"Last-Modified": ours})

    # Simulate what aiohttp's FileResponse.prepare() does (self.last_modified = file mtime).
    response.last_modified = 2_000_000_000

    assert response.headers["Last-Modified"] == ours


def test_pulp_file_response_never_emits_file_mtime(tmp_path):
    """Without a handler Last-Modified, filesystem mtime is still not advertised."""
    path = tmp_path / "artifact"
    path.write_text("data")
    response = PulpFileResponse(path)

    response.last_modified = 1_000_000_000

    assert "Last-Modified" not in response.headers


def test_pulp_file_response_ignores_etag_when_owning_validator(tmp_path):
    """mtime ETags are not advertised when Pulp owns Last-Modified."""
    path = tmp_path / "artifact"
    path.write_text("data")
    response = PulpFileResponse(path, headers={"Last-Modified": http_date(1_000_000_000)})

    response.etag = "abc123"

    assert "ETag" not in response.headers


def test_pulp_file_response_never_emits_mtime_etag(tmp_path):
    """mtime ETags are not advertised even when the handler omitted Last-Modified."""
    path = tmp_path / "artifact"
    path.write_text("data")
    response = PulpFileResponse(path)

    response.etag = "abc123"

    assert "ETag" not in response.headers


def test_pulp_file_response_does_not_304_on_stale_file_mtime(tmp_path):
    """IMS between file mtime and Pulp Last-Modified must not 304 on mtime."""
    path = tmp_path / "artifact"
    path.write_bytes(b"payload")
    os.utime(path, (1_000_000_000, 1_000_000_000))  # 2001-09-09

    pulp_lm = http_date(datetime(2024, 6, 1, tzinfo=timezone.utc).timestamp())
    # After file mtime, before Pulp Last-Modified — stock FileResponse would 304.
    ims = http_date(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp())
    request = make_mocked_request("GET", "/", headers={"If-Modified-Since": ims})

    pulp = PulpFileResponse(str(path), headers={"Last-Modified": pulp_lm})
    result, fobj, _st, _enc = pulp._make_response(request, "")
    try:
        assert result is _FileResponseResult.SEND_FILE
    finally:
        if fobj:
            fobj.close()
    assert pulp.headers["Last-Modified"] == pulp_lm

    stock = FileResponse(str(path))
    result, fobj, _st, _enc = stock._make_response(
        make_mocked_request("GET", "/", headers={"If-Modified-Since": ims}), ""
    )
    try:
        assert result is _FileResponseResult.NOT_MODIFIED
    finally:
        if fobj:
            fobj.close()


def test_pulp_file_response_without_validator_does_not_304_on_mtime(tmp_path):
    """Publish-generated files with no Pulp Last-Modified must not 304 against mtime."""
    path = tmp_path / "artifact"
    path.write_bytes(b"payload")
    os.utime(path, (1_000_000_000, 1_000_000_000))
    ims = http_date(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp())
    request = make_mocked_request("GET", "/", headers={"If-Modified-Since": ims})

    response = PulpFileResponse(str(path))
    result, fobj, _st, _enc = response._make_response(request, "")
    try:
        assert result is _FileResponseResult.SEND_FILE
    finally:
        if fobj:
            fobj.close()
    assert "Last-Modified" not in response.headers


def test_pulp_file_response_does_not_blank_if_range(tmp_path):
    """If-Range stays available so aiohttp can refuse a stale Range instead of a corrupt 206."""
    path = tmp_path / "artifact"
    path.write_bytes(b"payload")
    os.utime(path, (1_000_000_000, 1_000_000_000))
    pulp_lm = http_date(1_700_000_000)
    if_range = http_date(1_000_000_000)
    request = make_mocked_request(
        "GET",
        "/",
        headers={
            "If-Range": if_range,
            "Range": "bytes=0-1",
            "If-Modified-Since": if_range,
        },
    )
    response = PulpFileResponse(str(path), headers={"Last-Modified": pulp_lm})
    result, fobj, _st, _enc = response._make_response(request, "")
    if fobj:
        fobj.close()

    assert result is _FileResponseResult.SEND_FILE
    assert request.if_range is not None
    assert request.if_modified_since is None
