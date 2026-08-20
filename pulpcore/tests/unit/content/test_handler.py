import uuid
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from aiohttp.web_exceptions import (
    HTTPFound,
    HTTPMovedPermanently,
    HTTPNotModified,
    HTTPRequestRangeNotSatisfiable,
)
from asgiref.sync import sync_to_async
from django.db import IntegrityError
from django.test import override_settings
from django.utils.http import http_date
from django_guid import clear_guid, set_guid
from multidict import CIMultiDict

from pulpcore.app.models import AppStatus
from pulpcore.constants import TASK_STATES
from pulpcore.content.handler import CheckpointListings, Handler, PathNotResolved
from pulpcore.plugin.models import (
    Artifact,
    Content,
    ContentArtifact,
    Distribution,
    Publication,
    Remote,
    RemoteArtifact,
    Repository,
    RepositoryVersion,
)


@pytest.fixture
def download_result_mock(tmp_path):
    dr = Mock()
    dr.artifact_attributes = {"size": 0}
    for digest_type in Artifact.DIGEST_FIELDS:
        dr.artifact_attributes[digest_type] = "abc123"
    tmp_file = tmp_path / str(uuid.uuid4())
    tmp_file.write_text("abc123")
    dr.path = str(tmp_file)
    return dr


@pytest.fixture
def c1(db):
    return Content.objects.create()


@pytest.fixture
def ca1(c1):
    return ContentArtifact.objects.create(artifact=None, content=c1, relative_path="c1")


@pytest.fixture
def ra1(ca1):
    return Mock(content_artifact=ca1)


@pytest.fixture
def c2(db):
    return Content.objects.create()


@pytest.fixture
def ca2(c2):
    return ContentArtifact.objects.create(artifact=None, content=c2, relative_path="c1")


@pytest.fixture
def ra2(ca2):
    return Mock(content_artifact=ca2)


@pytest.fixture
def repo():
    return Repository.objects.create(name=str(uuid.uuid4()))


@pytest.fixture
def repo_version_1(repo):
    return RepositoryVersion.objects.create(repository=repo, number=1)


@pytest.fixture
def repo_version_2(repo):
    return RepositoryVersion.objects.create(repository=repo, number=2)


@pytest.fixture
def repo_version_3(repo):
    return RepositoryVersion.objects.create(repository=repo, number=3)


@pytest.fixture
def checkpoint_distribution(repo):
    return Distribution.objects.create(
        name=str(uuid.uuid4()), base_path=str(uuid.uuid4()), repository=repo, checkpoint=True
    )


@pytest.fixture
def checkpoint_publication_1(repo_version_1):
    publication = Publication.objects.create(repository_version=repo_version_1, checkpoint=True)
    # Avoid creating publications in the future, which would cause a 404
    publication.pulp_created = publication.pulp_created - timedelta(seconds=6)
    publication.save()

    return publication


@pytest.fixture
def noncheckpoint_publication(repo_version_2, checkpoint_publication_1):
    publication = Publication.objects.create(repository_version=repo_version_2, checkpoint=False)
    publication.pulp_created = checkpoint_publication_1.pulp_created + timedelta(seconds=2)
    publication.save()

    return publication


@pytest.fixture
def checkpoint_publication_2(repo_version_3, noncheckpoint_publication):
    publication = Publication.objects.create(repository_version=repo_version_3, checkpoint=True)
    publication.pulp_created = noncheckpoint_publication.pulp_created + timedelta(seconds=2)
    publication.save()

    return publication


def test_save_artifact(c1, ra1, download_result_mock):
    """Artifact needs to be created."""
    handler = Handler()
    content_artifacts = handler._save_artifact(download_result_mock, ra1)
    c1 = Content.objects.get(pk=c1.pk)
    assert content_artifacts is not None
    assert ra1.content_artifact.relative_path in content_artifacts
    artifact = content_artifacts[ra1.content_artifact.relative_path].artifact
    assert c1._artifacts.get().pk == artifact.pk


def test_save_artifact_artifact_already_exists(c2, ra1, ra2, download_result_mock):
    """Artifact turns out to already exist."""
    cch = Handler()
    new_content_artifacts = cch._save_artifact(download_result_mock, ra1)

    existing_content_artifacts = cch._save_artifact(download_result_mock, ra2)
    c2 = Content.objects.get(pk=c2.pk)
    assert ra1.content_artifact.relative_path in new_content_artifacts
    assert ra2.content_artifact.relative_path in existing_content_artifacts
    new_artifact = new_content_artifacts[ra1.content_artifact.relative_path]
    existing_artifact = existing_content_artifacts[ra2.content_artifact.relative_path]
    assert new_artifact.artifact.pk == existing_artifact.artifact.pk
    assert c2._artifacts.get().pk == existing_artifact.artifact.pk


# Test pull through features
@pytest.fixture
def remote123(db):
    return Remote.objects.create(name="123", url="https://123")


@pytest.fixture
def request123():
    return Mock(match_info={"path": "c123"})


# pytest-django fixtures does not work when testing async code
async def create_artifact(tmp_path):
    tmp_file = tmp_path / str(uuid.uuid4())
    tmp_file.write_text(str(tmp_file))
    artifact = Artifact.init_and_validate(str(tmp_file))
    await artifact.asave()
    return artifact


async def create_content():
    return await Content.objects.acreate()


async def create_content_artifact(content):
    return await ContentArtifact.objects.acreate(
        artifact=None, content=content, relative_path="c123"
    )


async def create_remote():
    return await Remote.objects.acreate(name=str(uuid.uuid4()), url="https://123")


async def create_remote_artifact(remote, ca):
    return await RemoteArtifact.objects.acreate(
        remote=remote, url="https://123/c123", content_artifact=ca
    )


async def create_repository():
    return await Repository.objects.acreate(name=str(uuid.uuid4()))


async def create_distribution(remote, repository=None):
    name = str(uuid.uuid4())
    return await Distribution.objects.acreate(
        name=name, base_path=name, remote=remote, repository=repository
    )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_pull_through_remote_artifact_exists(request123, tmp_path):
    """Remote Artifact already exists, stream or serve associated content."""
    handler = Handler()
    handler._stream_content_artifact = AsyncMock()

    # Setup content w/ remote artifact
    content = await create_content()
    ca = await create_content_artifact(content)
    remote = await create_remote()
    await create_remote_artifact(remote, ca)
    distro = await create_distribution(remote)

    # Check that the handler finds the on-demand CA and calls the stream method
    try:
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        handler._stream_content_artifact.assert_called_once()
        assert ca in handler._stream_content_artifact.call_args[0]

        # Manually save artifact for content_artifact
        tmp_file = tmp_path / str(uuid.uuid4())
        tmp_file.write_text(str(tmp_file))
        artifact = Artifact.init_and_validate(str(tmp_file))
        await artifact.asave()

        ca.artifact = artifact
        await ca.asave()
        handler._serve_content_artifact = AsyncMock()

        # Check that the handler finds the CA and calls the serve method
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        handler._serve_content_artifact.assert_called_once()
        assert ca in handler._serve_content_artifact.call_args[0]
    finally:
        # Cleanup since this test isn't using fixtures
        await content.adelete()
        await remote.adelete()
        await distro.adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_pull_through_new_remote_artifacts(request123, monkeypatch):
    """Remote Artifact doesn't exist, create and stream content."""
    handler = Handler()
    handler._stream_remote_artifact = AsyncMock()

    remote = await create_remote()
    monkeypatch.setattr(Remote, "get_remote_artifact_content_type", Mock(return_value=Content))
    distro = await create_distribution(remote)

    try:
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        remote.get_remote_artifact_content_type.assert_called_once_with("c123")
        handler._stream_remote_artifact.assert_called_once()

        args, kwargs = handler._stream_remote_artifact.call_args
        assert kwargs.get("save_artifact", None) is True
        ra = args[2]
        assert isinstance(ra, RemoteArtifact)
        assert ra.remote == remote
        assert ra.url == f"{remote.url}/c123"
        assert ra.content_artifact.relative_path == "c123"
    finally:
        await remote.adelete()
        await distro.adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_pull_through_metadata_file(request123, monkeypatch):
    """Requested path is for a metadata file. Don't save response."""
    handler = Handler()
    handler._stream_remote_artifact = AsyncMock()

    remote = await create_remote()
    monkeypatch.setattr(Remote, "get_remote_artifact_content_type", Mock(return_value=None))
    distro = await create_distribution(remote)

    try:
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        remote.get_remote_artifact_content_type.assert_called_once_with("c123")
        handler._stream_remote_artifact.assert_called_once()

        _, kwargs = handler._stream_remote_artifact.call_args
        assert kwargs.get("save_artifact", None) is False
    finally:
        await remote.adelete()
        await distro.adelete()


def test_pull_through_save_single_artifact_content(
    remote123, request123, download_result_mock, monkeypatch
):
    """Ensure single-artifact content is properly saved on pull-through."""
    handler = Handler()
    remote123.get_remote_artifact_content_type = Mock(return_value=Content)
    content_init_mock = Mock(return_value=Content())
    monkeypatch.setattr(Content, "init_from_artifact_and_relative_path", content_init_mock)
    ca = ContentArtifact(relative_path="c123")
    ra = RemoteArtifact(url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca)

    # Content is saved during handler._save_artifact
    content_artifacts = handler._save_artifact(download_result_mock, ra, request=request123)
    artifact = content_artifacts[ra.content_artifact.relative_path].artifact

    remote123.get_remote_artifact_content_type.assert_called_once_with("c123")
    content_init_mock.assert_called_once_with(artifact, "c123")

    # Assert the CA and RA are properly saved
    ca = artifact.content_memberships.first()
    assert ca.content is not None
    assert ca.relative_path == "c123"
    ra = RemoteArtifact.objects.filter(
        url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca
    ).first()
    assert ra is not None


def test_pull_through_save_multi_artifact_content(
    remote123, request123, download_result_mock, monkeypatch, tmp_path
):
    """Ensure multi-artifact content is properly saved on pull-through."""
    handler = Handler()
    remote123.get_remote_artifact_content_type = Mock(return_value=Content)

    tmp_file = tmp_path / str(uuid.uuid4())
    tmp_file.write_text(str(tmp_file))
    artifact123 = Artifact.init_and_validate(str(tmp_file))
    artifact123.save()

    def content_init(art, path):
        return Content(), {path: artifact123, path + "abc": art}

    monkeypatch.setattr(Content, "init_from_artifact_and_relative_path", content_init)
    ca = ContentArtifact(relative_path="c123")
    ra = RemoteArtifact(url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca)

    content_artifacts = handler._save_artifact(download_result_mock, ra, request123)
    ca1 = content_artifacts["c123"]
    ca2 = content_artifacts["c123abc"]
    assert ca1.content is not None
    assert ca2.content == ca1.content
    assert ca1.artifact == artifact123

    artifacts = set(ca1.content._artifacts.all())
    assert len(artifacts) == 2
    assert {ca2.artifact, artifact123} == artifacts


def test_pull_through_save_single_artifact_on_demand_content(
    remote123, request123, download_result_mock, monkeypatch
):
    """Ensure single-artifact content is properly saved on pull-through."""
    handler = Handler()
    remote123.get_remote_artifact_content_type = Mock(return_value=Content)
    content = Content.objects.create()
    content.save = Mock(side_effect=IntegrityError)
    content_init_mock = Mock(return_value=content)
    monkeypatch.setattr(Content, "init_from_artifact_and_relative_path", content_init_mock)
    monkeypatch.setattr(Content.objects, "get", Mock(return_value=content))
    ca = ContentArtifact(relative_path="c123")
    ra = RemoteArtifact(url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca)

    # Content is saved during handler._save_artifact
    content_artifacts = handler._save_artifact(download_result_mock, ra, request=request123)
    artifact = content_artifacts[ra.content_artifact.relative_path].artifact

    remote123.get_remote_artifact_content_type.assert_called_once_with("c123")
    content_init_mock.assert_called_once_with(artifact, "c123")
    content.save.assert_called_once()
    Content.objects.get.assert_called_once()

    # Assert the CA and RA are properly saved
    ca = artifact.content_memberships.first()
    assert ca.content == content
    assert ca.relative_path == "c123"
    ra = RemoteArtifact.objects.filter(
        url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca
    ).first()
    assert ra is not None

    # Test on-demand were CA is updated with downloaded artifact
    ra.delete()
    ca.artifact = None
    ca.save()

    ca = ContentArtifact(relative_path="c123")
    ra = RemoteArtifact(url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca)
    content_artifacts = handler._save_artifact(download_result_mock, ra, request=request123)
    assert artifact == content_artifacts[ra.content_artifact.relative_path].artifact

    # Assert the CA and RA are properly saved
    ca = artifact.content_memberships.first()
    assert ca.content == content
    assert ca.relative_path == "c123"
    ra = RemoteArtifact.objects.filter(
        url=f"{remote123.url}/c123", remote=remote123, content_artifact=ca
    ).first()
    assert ra is not None


@pytest.mark.django_db
def test_handle_checkpoint_listing(
    monkeypatch,
    checkpoint_distribution,
    checkpoint_publication_1,
    noncheckpoint_publication,
    checkpoint_publication_2,
):
    """Checkpoint listing is generated correctly."""
    # Extract the pulp_created timestamps
    checkpoint_pub_1_ts = Handler._format_checkpoint_timestamp(
        checkpoint_publication_1.pulp_created
    )
    noncheckpoint_pub_ts = Handler._format_checkpoint_timestamp(
        noncheckpoint_publication.pulp_created
    )
    checkpoint_pub_2_ts = Handler._format_checkpoint_timestamp(
        checkpoint_publication_2.pulp_created
    )

    # Mock the render_html function to capture the checkpoint list
    original_render_html = Handler.render_html
    checkpoint_list = None

    def mock_render_html(directory_list, dates=None, path=None):
        nonlocal checkpoint_list
        html = original_render_html(directory_list, dates=dates, path=path)
        checkpoint_list = directory_list
        return html

    render_html_mock = Mock(side_effect=mock_render_html)
    monkeypatch.setattr(Handler, "render_html", render_html_mock)

    with pytest.raises(CheckpointListings):
        Handler._select_checkpoint_publication(checkpoint_distribution, "")
    assert len(checkpoint_list) == 2
    assert f"{checkpoint_pub_1_ts}/" in checkpoint_list, (
        f"{checkpoint_pub_1_ts} not found in error body"
    )
    assert f"{checkpoint_pub_2_ts}/" in checkpoint_list, (
        f"{checkpoint_pub_2_ts} not found in error body"
    )
    assert f"{noncheckpoint_pub_ts}/" not in checkpoint_list, (
        f"{noncheckpoint_pub_ts} found in error body"
    )


@pytest.mark.django_db
def test_handle_checkpoint_exact_ts(
    checkpoint_distribution,
    checkpoint_publication_1,
    noncheckpoint_publication,
    checkpoint_publication_2,
):
    """Checkpoint is correctly served when using exact timestamp."""
    checkpoint_pub_2_ts = Handler._format_checkpoint_timestamp(
        checkpoint_publication_2.pulp_created
    )
    publication = Handler._select_checkpoint_publication(
        checkpoint_distribution, f"{checkpoint_pub_2_ts}/"
    )

    assert publication is not None
    assert publication == checkpoint_publication_2


@pytest.mark.django_db
def test_handle_checkpoint_invalid_ts(
    checkpoint_distribution,
    checkpoint_publication_1,
):
    """Invalid checkpoint timestamp raises PathNotResolved."""
    with pytest.raises(PathNotResolved):
        Handler._select_checkpoint_publication(checkpoint_distribution, "99990115T181699Z/")

    with pytest.raises(PathNotResolved):
        Handler._select_checkpoint_publication(checkpoint_distribution, "invalid_ts/")


@pytest.mark.django_db
def test_handle_checkpoint_arbitrary_ts(
    checkpoint_distribution,
    checkpoint_publication_1,
    noncheckpoint_publication,
    checkpoint_publication_2,
):
    """Checkpoint is correctly served when using an arbitrary timestamp."""
    request_ts = Handler._format_checkpoint_timestamp(
        checkpoint_publication_1.pulp_created + timedelta(seconds=3)
    )
    with pytest.raises(HTTPMovedPermanently) as excinfo:
        Handler._select_checkpoint_publication(checkpoint_distribution, f"{request_ts}/")
    redirect_location = excinfo.value.location

    with pytest.raises(HTTPMovedPermanently) as excinfo:
        Handler._redirect_sub_path(
            f"{checkpoint_distribution.base_path}"
            f"/{Handler._format_checkpoint_timestamp(checkpoint_publication_1.pulp_created)}/"
        )
    expected_location = excinfo.value.location

    assert redirect_location == expected_location, (
        f"Unexpected redirect location: {redirect_location}"
    )


@pytest.mark.django_db
def test_handle_checkpoint_before_first_ts(
    checkpoint_distribution,
    checkpoint_publication_1,
):
    """Checkpoint timestamp before the first checkpoint raises PathNotResolved.."""
    request_ts = Handler._format_checkpoint_timestamp(
        checkpoint_publication_1.pulp_created - timedelta(seconds=1)
    )
    with pytest.raises(PathNotResolved):
        Handler._select_checkpoint_publication(checkpoint_distribution, f"{request_ts}/")


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_pull_through_repository_add(request123, monkeypatch):
    """Test that repository adding is called when supported."""
    handler = Handler()
    handler._stream_content_artifact = AsyncMock()

    content = await create_content()
    ca = await create_content_artifact(content)
    remote = await create_remote()
    await create_remote_artifact(remote, ca)
    repo = await create_repository()
    monkeypatch.setattr(Remote, "get_remote_artifact_content_type", Mock(return_value=Content))
    monkeypatch.setattr(Repository, "async_pull_through_add_content", AsyncMock())
    distro = await create_distribution(remote, repository=repo)

    try:
        # Assert with Repository.PULL_THROUGH_SUPPORTED=False the method isn't called
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        handler._stream_content_artifact.assert_called_once()
        assert ca in handler._stream_content_artifact.call_args[0]
        repo.async_pull_through_add_content.assert_not_called()

        # Now set PULL_THROUGH_SUPPORTED=True and see the method is called with CA
        monkeypatch.setattr(Repository, "PULL_THROUGH_SUPPORTED", True)
        handler._stream_content_artifact.reset_mock()
        await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        handler._stream_content_artifact.assert_called_once()
        assert ca in handler._stream_content_artifact.call_args[0]
        repo.async_pull_through_add_content.assert_called_once()
        assert ca in repo.async_pull_through_add_content.call_args[0]
    finally:
        await content.adelete()
        await repo.adelete()
        await remote.adelete()
        await distro.adelete()


@pytest_asyncio.fixture
async def app_status(monkeypatch):
    monkeypatch.setattr(AppStatus.objects, "_current_app_status", None)
    app_status = await AppStatus.objects.acreate(app_type="api", name="test_runner")
    yield app_status
    await app_status.adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.parametrize("repeat", (1, 2))
async def test_app_status_fixture_is_reusable(app_status, repeat):
    # testing this because AppStatus handles global process state
    assert app_status


def test_render_html_colon_in_name():
    """Links with colons in the name should use './' prefix to avoid being treated as a scheme."""
    html = Handler.render_html(["copr-pull-requests:pr:3825/"])
    assert '<a href="./copr-pull-requests:pr:3825/">copr-pull-requests:pr:3825/</a>' in html


def test_render_html_normal_name():
    """Normal directory names should also get the './' prefix."""
    html = Handler.render_html(["simple-dir/"])
    assert '<a href="./simple-dir/">simple-dir/</a>' in html


async def _served_content_artifact(tmp_path):
    """Create a ContentArtifact with a real (present) Artifact for serving tests."""
    content = await create_content()
    ca = await create_content_artifact(content)
    ca.artifact = await create_artifact(tmp_path)
    await ca.asave()
    return ca


_LAST_MODIFIED = datetime(2020, 1, 1, tzinfo=dt_timezone.utc)
_IMS_AFTER = http_date(datetime(2021, 1, 1, tzinfo=dt_timezone.utc).timestamp())
_CACHE_CONTROL = "public, max-age=0, must-revalidate"


class _UnsatisfiableRange:
    @property
    def start(self):
        raise ValueError()

    stop = None


def _request(*, ims=None, http_range=None, headers=None, range_header=None):
    hdrs = dict(headers or {})
    if ims is not None:
        hdrs["If-Modified-Since"] = ims
    if range_header is not None:
        hdrs["Range"] = range_header
    return Mock(
        method="GET",
        http_range=http_range if http_range is not None else Mock(start=None, stop=None),
        headers=hdrs,
    )


async def _handler_with_built_response(tmp_path, monkeypatch, built=None):
    handler = Handler()
    ca = await _served_content_artifact(tmp_path)
    if built is None:
        built = Mock(headers={}, status=200)
    monkeypatch.setattr(handler, "_build_response_from_content_artifact", Mock(return_value=built))
    return handler, ca, built


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_sets_last_modified(tmp_path, monkeypatch):
    """A Last-Modified header is set on the served response when last_modified is provided."""
    handler, ca, built = await _handler_with_built_response(tmp_path, monkeypatch)

    with override_settings(CACHE_ENABLED=False):
        response = await handler._serve_content_artifact(
            ca, {}, _request(), last_modified=_LAST_MODIFIED
        )

    assert response is built
    assert response.headers["Last-Modified"] == http_date(_LAST_MODIFIED.timestamp())


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_returns_304_when_not_modified(tmp_path, monkeypatch):
    """A matching If-Modified-Since yields a bodyless 304 on the non-cached path."""
    handler, ca, _built = await _handler_with_built_response(tmp_path, monkeypatch)

    with override_settings(CACHE_ENABLED=False):
        with pytest.raises(HTTPNotModified) as exc:
            await handler._serve_content_artifact(
                ca, {}, _request(ims=_IMS_AFTER), last_modified=_LAST_MODIFIED
            )

    assert exc.value.headers["Last-Modified"] == http_date(_LAST_MODIFIED.timestamp())


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_no_304_when_cache_enabled(tmp_path, monkeypatch):
    """When caching is enabled the cache layer owns the 304 decision, so the full response
    (with Last-Modified) is returned even for a matching If-Modified-Since."""
    handler, ca, built = await _handler_with_built_response(tmp_path, monkeypatch)

    with override_settings(CACHE_ENABLED=True):
        response = await handler._serve_content_artifact(
            ca, {}, _request(ims=_IMS_AFTER), last_modified=_LAST_MODIFIED
        )

    assert response is built
    assert response.headers["Last-Modified"] == http_date(_LAST_MODIFIED.timestamp())


def test_response_headers_sets_cache_control():
    """All content responses instruct edge caches to revalidate on every use."""
    headers = Handler.response_headers("path/to/file.iso")
    assert headers["Cache-Control"] == _CACHE_CONTROL


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_304_echoes_cache_control(tmp_path, monkeypatch):
    """The bodyless 304 echoes Cache-Control (and Last-Modified) so caches keep revalidating."""
    built = Mock(headers={"Cache-Control": _CACHE_CONTROL})
    handler, ca, _built = await _handler_with_built_response(tmp_path, monkeypatch, built=built)

    with override_settings(CACHE_ENABLED=False):
        with pytest.raises(HTTPNotModified) as exc:
            await handler._serve_content_artifact(
                ca, {}, _request(ims=_IMS_AFTER), last_modified=_LAST_MODIFIED
            )

    assert exc.value.headers["Last-Modified"] == http_date(_LAST_MODIFIED.timestamp())
    assert exc.value.headers["Cache-Control"] == _CACHE_CONTROL


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_redirect_has_no_last_modified(tmp_path, monkeypatch):
    """Redirect (object-storage) responses are left untouched: no Last-Modified, no 304."""
    redirect = HTTPFound(
        "http://example.test/redirect",
        headers={"Cache-Control": _CACHE_CONTROL},
    )
    handler, ca, _built = await _handler_with_built_response(tmp_path, monkeypatch, built=redirect)

    with override_settings(CACHE_ENABLED=False):
        with pytest.raises(HTTPFound) as exc:
            await handler._serve_content_artifact(ca, {}, _request(), last_modified=_LAST_MODIFIED)

    assert "Last-Modified" not in exc.value.headers
    assert "Cache-Control" not in exc.value.headers


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_build_response_redirect_omits_cache_control(tmp_path, monkeypatch):
    """Object-storage 302s are constructed without Cache-Control: public."""
    handler = Handler()
    ca = await _served_content_artifact(tmp_path)
    domain = Mock(
        storage_class="storages.backends.s3.S3Storage",
        redirect_to_object_storage=True,
        get_storage=Mock(return_value=Mock(url=Mock(return_value="https://s3.example/obj"))),
    )
    monkeypatch.setattr("pulpcore.content.handler.get_domain", lambda: domain)
    headers = CIMultiDict({"Cache-Control": _CACHE_CONTROL})

    response = handler._build_response_from_content_artifact(ca, headers, Mock(method="GET"))

    assert isinstance(response, HTTPFound)
    assert "Cache-Control" not in response.headers


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.parametrize(
    "http_range,range_header",
    [
        (_UnsatisfiableRange(), "bytes=abc"),
        (Mock(start=0, stop=1), "bytes=0-1"),
    ],
)
async def test_serve_content_artifact_304_beats_range(
    tmp_path, monkeypatch, http_range, range_header
):
    """A matching If-Modified-Since 304s; Range must not become 416 or 206."""
    handler, ca, built = await _handler_with_built_response(tmp_path, monkeypatch)
    request = _request(ims=_IMS_AFTER, http_range=http_range, range_header=range_header)

    with override_settings(CACHE_ENABLED=False):
        with pytest.raises(HTTPNotModified) as exc:
            await handler._serve_content_artifact(ca, {}, request, last_modified=_LAST_MODIFIED)

    assert exc.value.status == 304
    assert exc.value.headers["Last-Modified"] == http_date(_LAST_MODIFIED.timestamp())
    assert built.status == 200


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_content_artifact_invalid_range_is_416_when_modified(tmp_path, monkeypatch):
    """Without a matching If-Modified-Since, an unsatisfiable Range is still 416."""
    handler, ca, _built = await _handler_with_built_response(tmp_path, monkeypatch)

    with override_settings(CACHE_ENABLED=False):
        with pytest.raises(HTTPRequestRangeNotSatisfiable):
            await handler._serve_content_artifact(
                ca, {}, _request(http_range=_UnsatisfiableRange()), last_modified=_LAST_MODIFIED
            )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_ca_streams_304_before_remote_fetch(monkeypatch):
    """On-demand units 304 before streaming when If-Modified-Since covers Last-Modified."""
    handler = Handler()
    content = await create_content()
    ca = await create_content_artifact(content)
    last_modified = _LAST_MODIFIED
    monkeypatch.setattr(handler, "_content_last_modified", AsyncMock(return_value=last_modified))
    handler._stream_content_artifact = AsyncMock(return_value="streamed")
    headers = {"Cache-Control": _CACHE_CONTROL}
    request = Mock(headers={"If-Modified-Since": http_date(last_modified.timestamp())})

    with pytest.raises(HTTPNotModified) as exc:
        await handler._serve_ca(ca, headers, request, repository_version="rv")

    handler._stream_content_artifact.assert_not_awaited()
    assert exc.value.headers["Last-Modified"] == http_date(last_modified.timestamp())
    assert exc.value.headers["Cache-Control"] == "public, max-age=0, must-revalidate"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_ca_looks_up_last_modified_and_serves(tmp_path, monkeypatch):
    """_serve_ca looks up pulp_created and passes it to `_serve_content_artifact` (not headers)."""
    handler = Handler()
    ca = await _served_content_artifact(tmp_path)
    last_modified = datetime(2020, 1, 1, tzinfo=dt_timezone.utc)
    monkeypatch.setattr(handler, "_content_last_modified", AsyncMock(return_value=last_modified))
    handler._serve_content_artifact = AsyncMock(return_value="served")
    headers = {}
    request = Mock()

    result = await handler._serve_ca(ca, headers, request, publication="pub")

    assert result == "served"
    assert "Last-Modified" not in headers
    handler._content_last_modified.assert_awaited_once_with(
        ca, publication="pub", repository_version=None
    )
    handler._serve_content_artifact.assert_awaited_once_with(
        ca, headers, request, last_modified=last_modified
    )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_serve_ca_streams_with_last_modified_when_artifact_missing(monkeypatch):
    """On-demand units stamp Last-Modified on the stream headers, since there is no redirect."""
    handler = Handler()
    content = await create_content()
    ca = await create_content_artifact(content)
    last_modified = datetime(2020, 1, 1, tzinfo=dt_timezone.utc)
    monkeypatch.setattr(handler, "_content_last_modified", AsyncMock(return_value=last_modified))
    handler._stream_content_artifact = AsyncMock(return_value="streamed")
    headers = {}
    request = Mock(headers={})

    result = await handler._serve_ca(ca, headers, request, repository_version="rv")

    assert result == "streamed"
    assert headers["Last-Modified"] == http_date(last_modified.timestamp())
    handler._stream_content_artifact.assert_awaited_once()
    stream_request, stream_response, stream_ca = handler._stream_content_artifact.call_args.args
    assert stream_request is request
    assert stream_ca is ca
    assert stream_response.headers["Last-Modified"] == http_date(last_modified.timestamp())


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_match_and_stream_content_handler_ca_uses_serve_ca(tmp_path, monkeypatch):
    """A ContentArtifact returned by content_handler is served with the distro's repo version."""
    handler = Handler()
    ca = await _served_content_artifact(tmp_path)
    distro = Mock(
        base_path="bp",
        checkpoint=False,
        content_handler=Mock(return_value=ca),
        content_headers_for=Mock(return_value={}),
        get_repository_publication_and_version=Mock(return_value=(None, "rv", "pub")),
    )
    monkeypatch.setattr(Handler, "_match_distribution", Mock(return_value=distro))
    monkeypatch.setattr(Handler, "_permit", Mock(return_value=False))
    handler._serve_ca = AsyncMock(return_value="ok")
    request = Mock(path="/pulp/content/bp/c123")

    result = await handler._match_and_stream("bp/c123", request)

    assert result == "ok"
    handler._serve_ca.assert_awaited_once()
    assert handler._serve_ca.call_args.args[0] == ca
    assert handler._serve_ca.call_args.kwargs["publication"] == "pub"
    assert handler._serve_ca.call_args.kwargs["repository_version"] == "rv"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_match_and_stream_fallback_uses_serve_ca(tmp_path, monkeypatch):
    """Grace-period fallback serves the CA with the superseded publication for Last-Modified."""
    handler = Handler()
    ca = await _served_content_artifact(tmp_path)
    distro = Mock(
        base_path="bp",
        checkpoint=False,
        SERVE_FROM_PUBLICATION=True,
        content_handler=Mock(return_value=None),
        content_headers_for=Mock(return_value={}),
        get_repository_publication_and_version=Mock(return_value=(None, None, None)),
        get_fallback=Mock(return_value=(ca, "fallback_pub")),
        remote=None,
    )
    monkeypatch.setattr(Handler, "_match_distribution", Mock(return_value=distro))
    monkeypatch.setattr(Handler, "_permit", Mock(return_value=False))
    handler._serve_ca = AsyncMock(return_value="ok")
    request = Mock(path="/pulp/content/bp/c123")

    result = await handler._match_and_stream("bp/c123", request)

    assert result == "ok"
    distro.get_fallback.assert_called_once_with("c123")
    handler._serve_ca.assert_awaited_once()
    assert handler._serve_ca.call_args.args[0] == ca
    assert handler._serve_ca.call_args.kwargs["publication"] == "fallback_pub"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_match_and_stream_pull_through_uses_serve_ca(request123, tmp_path):
    """Pull-through with an existing ContentArtifact serves through ``_serve_ca``."""
    handler = Handler()
    content = await create_content()
    ca = await create_content_artifact(content)
    remote = await create_remote()
    await create_remote_artifact(remote, ca)
    repo = await create_repository()
    distro = await create_distribution(remote, repository=repo)
    handler._serve_ca = AsyncMock(return_value="ok")

    try:
        result = await handler._match_and_stream(f"{distro.base_path}/c123", request123)
        assert result == "ok"
        handler._serve_ca.assert_awaited_once()
        assert handler._serve_ca.call_args.args[0] == ca
        expected_rv = await sync_to_async(repo.latest_version)()
        assert handler._serve_ca.call_args.kwargs["repository_version"] == expected_rv
    finally:
        await content.adelete()
        await repo.adelete()
        await remote.adelete()
        await distro.adelete()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_async_pull_through_add(ca1, monkeypatch, app_status):
    set_guid(uuid.uuid4())  # required for creating a task, no easily mockable
    monkeypatch.setattr(
        "pulpcore.tasking.tasks.async_are_resources_available", AsyncMock(return_value=True)
    )
    monkeypatch.setattr("pulpcore.tasking.tasks.wakeup_worker", Mock())

    repo = await Repository.objects.acreate(name=str(uuid.uuid4()))
    try:
        task = await repo.async_pull_through_add_content(ca1)
        assert task.state == TASK_STATES.COMPLETED
    except Exception as e:
        task = None
        assert e is None
    finally:
        clear_guid()
        await repo.adelete()
        if task:
            await task.adelete()
