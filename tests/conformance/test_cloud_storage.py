# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""End-to-end checks for the cloud external-storage backends.

Driven against a local S3-compatible server rather than real AWS, so the test
is hermetic — but it is the real `aws-sdk-cpp` code path, including
pre-signing, which is the half a unit test cannot reach. A pointer batch
carries a pre-signed HTTPS URL, and the whole design rests on that URL being
fetchable by a client holding no credentials; nothing but an end-to-end run
proves it.

Two things must be present or the S3 group skips:

* a worker built with the S3 backend (``VCPKG_MANIFEST_FEATURES=s3`` plus
  ``-DVGI_RPC_WITH_S3=ON``), which the default build is not;
* an S3-compatible server: RustFS under Docker (pinned image, overridable
  with ``S3_TEST_IMAGE``), a ``minio`` binary named by ``MINIO_BIN``, or a
  `moto` server if ``moto[server]`` is installed.  RustFS validates SigV4,
  so a pre-signed URL that is not bound to its signature fails here.

With ``VGI_RPC_REQUIRE_CLOUD_TESTS=1`` (the cloud-storage CI lane) every one
of those skips is a failure instead: the lane once stayed green for weeks
with every S3 test skipped, after the minio/minio image stopped being
pullable.

The GCS group covers signing only, and says so. V4 signing is a local RSA
operation over a canonical string, so a throwaway key verifies it completely
without a Google account. The upload half is not reachable here: pointing the
client at an emulator makes it fall back to anonymous credentials, which have
no private key and push signing to a remote IAM call — so no configuration
exercises both halves at once locally.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import pytest

from conftest import spawn_http, worker_path

_BUCKET = "vgi-rpc-conformance"
_REQUIRED = os.environ.get("VGI_RPC_REQUIRE_CLOUD_TESTS") == "1"


def _skip(reason: str) -> None:
    """Skip, or fail when the lane requires every cloud test to run."""
    if _REQUIRED:
        pytest.fail(f"VGI_RPC_REQUIRE_CLOUD_TESTS=1 but: {reason}")
    pytest.skip(reason)
# RustFS 1.0.0-beta.12 (glibc), multi-architecture manifest; the image
# vgi-rpc-java's storage lane uses.
_RUSTFS_IMAGE = (
    "ghcr.io/rustfs/rustfs"
    "@sha256:29c02251c085cb04edce556304a9ec0f8fba0c40300266cf4f3d953783fe2450"
)
_ACCESS_KEY = "vgirpctest"
_SECRET_KEY = "vgirpctestsecret"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def s3_capable_worker() -> str:
    """Skip the module unless the worker under test has the S3 backend.

    Probed by asking it to serve an ``s3://`` URI: a build without the backend
    refuses at startup naming the missing feature, which is exactly what the
    storage factory promises to do.
    """
    proc = subprocess.Popen(
        [worker_path(), "--http", "--external-storage", "s3://probe/prefix",
         "--storage-endpoint", "http://127.0.0.1:1", "--storage-region", "us-east-1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        # A capable worker binds and prints its port, then serves forever; an
        # incapable one exits refusing the URI. Wait on the process, not on a
        # read, so neither outcome hangs the probe.
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        return worker_path()  # it started, so the backend is present
    finally:
        if proc.poll() is None:
            proc.kill()
    stderr = proc.stderr.read() if proc.stderr else b""
    if b"requires a build with the 's3' vcpkg feature" in stderr:
        _skip("worker was built without the S3 backend")
    _skip(f"S3 probe worker exited unexpectedly: {stderr.decode()[:200]}")


@pytest.fixture(scope="module")
def s3_endpoint(
    s3_capable_worker: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[str]:
    """A local S3-compatible server with the bucket created, yielding its URL."""
    try:
        import boto3
    except ImportError:
        _skip("boto3 is not installed")

    port = _free_port()
    proc: subprocess.Popen[bytes] | None = None
    container = ""
    minio = os.environ.get("MINIO_BIN")

    if minio:
        data = tmp_path_factory.mktemp("minio")
        proc = subprocess.Popen(
            [minio, "server", str(data), "--address", f"127.0.0.1:{port}",
             "--console-address", f"127.0.0.1:{_free_port()}"],
            env={**os.environ, "MINIO_ROOT_USER": _ACCESS_KEY,
                 "MINIO_ROOT_PASSWORD": _SECRET_KEY},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif shutil.which("docker"):
        container = f"vgi-rpc-rustfs-{port}"
        proc = subprocess.Popen(
            ["docker", "run", "--rm", "--name", container,
             "-p", f"127.0.0.1:{port}:9000",
             "-e", f"RUSTFS_ACCESS_KEY={_ACCESS_KEY}",
             "-e", f"RUSTFS_SECRET_KEY={_SECRET_KEY}",
             "-e", "RUSTFS_CONSOLE_ENABLE=false",
             "-e", "RUSTFS_OBS_LOGGER_LEVEL=error",
             os.environ.get("S3_TEST_IMAGE", _RUSTFS_IMAGE)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        try:
            import flask  # noqa: F401
        except ImportError:
            _skip("no Docker, MINIO_BIN or moto[server] is available")
        proc = subprocess.Popen(
            [sys.executable, "-m", "moto.server", "-p", str(port), "-H", "127.0.0.1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    endpoint = f"http://127.0.0.1:{port}"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=_ACCESS_KEY,
            aws_secret_access_key=_SECRET_KEY,
        )
        deadline = time.monotonic() + 120
        while True:
            try:
                client.create_bucket(Bucket=_BUCKET)
                break
            except Exception as exc:  # noqa: BLE001, PERF203
                if "BucketAlreadyOwnedByYou" in str(exc):
                    break
                if time.monotonic() > deadline:
                    _skip(f"no S3-compatible server became ready: {exc}")
                time.sleep(0.5)
        yield endpoint
    finally:
        if container:
            subprocess.run(["docker", "rm", "-f", container],
                           capture_output=True, check=False)
        if proc is not None:
            proc.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=10)
            if proc.poll() is None:
                proc.kill()


@pytest.fixture(scope="module")
def s3_client(s3_endpoint: str) -> object:
    """A boto3 client pointed at the local server, for asserting on the bucket."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
    )


@pytest.fixture(scope="module")
def s3_worker(s3_endpoint: str) -> Iterator[int]:
    """A worker externalising to the local S3 server, yielding its port."""
    env_keys = {
        "AWS_ACCESS_KEY_ID": _ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
        # The SDK would otherwise spend its connect timeout probing for
        # instance-metadata credentials that do not exist here.
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    saved = {k: os.environ.get(k) for k in env_keys}
    os.environ.update(env_keys)
    try:
        with spawn_http(
            "--external-storage", f"s3://{_BUCKET}/vgi-rpc",
            "--storage-endpoint", s3_endpoint,
            "--storage-region", "us-east-1",
            "--externalize-threshold", "4096",
        ) as port:
            yield port
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _connect(port: int) -> object:
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.external import ExternalLocationConfig
    from vgi_rpc.http import http_connect

    # The pre-signed URLs are plain http against a local endpoint, so the
    # client's HTTPS-only validator has to be relaxed for the test.
    return http_connect(
        ConformanceService,
        f"http://127.0.0.1:{port}",
        external_location=ExternalLocationConfig(url_validator=None),
    )


def test_capabilities_advertise_externalization(s3_worker: int) -> None:
    """A client learns from the headers that pointer batches are possible."""
    from vgi_rpc.http import http_capabilities

    caps = http_capabilities(f"http://127.0.0.1:{s3_worker}")
    assert caps.upload_url_support is True
    assert caps.max_request_bytes is not None and caps.max_request_bytes > 0


def test_small_payload_stays_inline(s3_worker: int) -> None:
    """Below the threshold nothing is uploaded, so nothing has to be fetched."""
    with _connect(s3_worker) as proxy:
        assert proxy.echo_string(value="small") == "small"


def test_large_payload_round_trips_through_s3(s3_worker: int) -> None:
    """The whole point: upload, pre-sign, and let the client fetch it back.

    The client holds no AWS credentials — it follows the pre-signed URL out of
    the pointer batch — so a pass here covers the signing as much as the upload.
    """
    big = "s3-round-trip " * 4000
    with _connect(s3_worker) as proxy:
        assert proxy.echo_large_string(value=big) == big


def test_objects_actually_land_in_the_bucket(s3_worker: int, s3_client: object) -> None:
    """Proves the payload went through S3 rather than staying inline.

    Without this, a server that quietly ignored its storage configuration and
    inlined everything would pass every test above.
    """

    def object_count() -> int:
        response = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix="vgi-rpc/")  # type: ignore[attr-defined]
        return int(response.get("KeyCount", 0))

    before = object_count()
    big = "z" * 32_000
    with _connect(s3_worker) as proxy:
        assert proxy.echo_large_string(value=big) == big
    assert object_count() > before, "nothing was uploaded — the payload stayed inline"


def test_upload_urls_are_usable_by_a_credential_free_client(s3_worker: int) -> None:
    """Vended URLs must work with nothing but an HTTP client.

    Distinct credentials per verb is why two are vended: a client given the
    pair can PUT its payload and GET it back, and nothing else.
    """
    import hashlib

    import httpx2

    from vgi_rpc.http import request_upload_urls, upload_put_headers

    # Upload URLs v2: each URL is signed for one declared (size, sha256) body.
    payload = b"client-vended upload contents"
    other = b"a second, distinct upload"
    urls = request_upload_urls(
        f"http://127.0.0.1:{s3_worker}",
        uploads=[(len(p), hashlib.sha256(p).hexdigest()) for p in (payload, other)],
    )
    assert len(urls) == 2
    assert urls[0].upload_url != urls[0].download_url, (
        "a pre-signing backend must vend one credential per verb"
    )

    put = httpx2.put(
        urls[0].upload_url, content=payload, headers=upload_put_headers(urls[0]), timeout=30.0
    )
    assert put.status_code in (200, 204), f"PUT failed: {put.status_code} {put.text[:200]}"

    got = httpx2.get(urls[0].download_url, timeout=30.0)
    assert got.status_code == 200
    assert got.content == payload


def test_grant_object_id_resolves_through_s3(s3_worker: int, s3_client: object) -> None:
    """Protocol 3.4: a grant names its S3 key, and its ref resolves via the object API.

    ``object_id`` is the key the PUT creates; ``resolve_argument_ref`` then
    makes the worker mint a presigned GET of that key server-side and verify
    the bytes -- the S3 half of ``ObjectStore.get``.  Deleting the key turns
    the next (cache-cold) resolve into ``arguments_ref_missing``.
    """
    import hashlib

    import httpx2

    from vgi_rpc.http import request_upload_urls, upload_put_headers
    from vgi_rpc.rpc import RpcError

    payload = b"an argument stored in S3 " * 64
    digest = hashlib.sha256(payload).hexdigest()
    (url,) = request_upload_urls(f"http://127.0.0.1:{s3_worker}", uploads=[(len(payload), digest)])
    assert url.object_id.startswith("vgi-rpc/"), f"object_id is not the S3 key: {url.object_id!r}"
    assert url.ref, "the grant carried no argument reference"
    put = httpx2.put(url.upload_url, content=payload, headers=upload_put_headers(url), timeout=30.0)
    assert put.status_code in (200, 204), f"PUT failed: {put.status_code} {put.text[:200]}"

    with _connect(s3_worker) as proxy:
        assert proxy.resolve_argument_ref(ref=url.ref, size=len(payload), sha256=digest) == digest

    gone = b"an S3 argument that is deleted " * 32
    gone_digest = hashlib.sha256(gone).hexdigest()
    (gone_url,) = request_upload_urls(
        f"http://127.0.0.1:{s3_worker}", uploads=[(len(gone), gone_digest)]
    )
    put = httpx2.put(
        gone_url.upload_url, content=gone, headers=upload_put_headers(gone_url), timeout=30.0
    )
    assert put.status_code in (200, 204)
    s3_client.delete_object(Bucket=_BUCKET, Key=gone_url.object_id)  # type: ignore[attr-defined]
    with _connect(s3_worker) as proxy, pytest.raises(RpcError) as raised:
        proxy.resolve_argument_ref(ref=gone_url.ref, size=len(gone), sha256=gone_digest)
    assert raised.value.error_kind == "arguments_ref_missing"


def _upload_url_for(port: int) -> str:
    import hashlib

    from vgi_rpc.http import request_upload_urls

    payload = b"addressing style probe"
    urls = request_upload_urls(
        f"http://127.0.0.1:{port}", uploads=[(len(payload), hashlib.sha256(payload).hexdigest())]
    )
    return urls[0].upload_url


def test_custom_endpoint_presigns_path_style_by_default(s3_worker: int, s3_endpoint: str) -> None:
    """MinIO and Ceph need path-style URLs on the configured endpoint's host."""
    from urllib.parse import urlsplit

    url = urlsplit(_upload_url_for(s3_worker))
    assert f"{url.scheme}://{url.netloc}" == s3_endpoint
    assert url.path.startswith(f"/{_BUCKET}/")


def test_addressing_style_virtual_presigns_the_bucket_host(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``virtual`` puts the bucket in the host (Wasabi, R2 and AWS accept it).

    Pre-signing is local, so only the URL is checked: ``<bucket>.localhost``
    need not resolve.
    """
    from urllib.parse import urlsplit

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET_KEY)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")

    port = urlsplit(s3_endpoint).port
    with spawn_http(
        "--external-storage", f"s3://{_BUCKET}/vgi-rpc",
        "--storage-endpoint", f"http://localhost:{port}",
        "--storage-region", "auto",
        "--storage-addressing-style", "virtual",
    ) as worker:
        url = urlsplit(_upload_url_for(worker))
    assert url.netloc == f"{_BUCKET}.localhost:{port}"
    assert not url.path.startswith(f"/{_BUCKET}/")


def test_unknown_addressing_style_is_refused_at_startup(s3_capable_worker: str) -> None:
    proc = subprocess.run(
        [s3_capable_worker, "--http", "--external-storage", f"s3://{_BUCKET}/p",
         "--storage-endpoint", "http://127.0.0.1:1", "--storage-addressing-style", "dns"],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode != 0
    assert b"s3_addressing_style" in proc.stderr


# ---------------------------------------------------------------------------
# VGI_STORAGE_* (direct-storage contract v1), through --storage-from-env
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _env_worker(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> Iterator[int]:
    """A worker that takes its storage from VGI_STORAGE_* (configure_http_from_env)."""
    for name in list(os.environ):
        if name.startswith(("VGI_STORAGE_", "VGI_BLOB_VENDOR")):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with spawn_http("--storage-from-env") as port:
        yield port


def _s3_env(endpoint: str, **extra: str) -> dict[str, str]:
    return {
        "VGI_STORAGE_S3_BUCKET": _BUCKET,
        "VGI_STORAGE_S3_ENDPOINT": endpoint,
        "VGI_STORAGE_S3_PREFIX": "env/",
        "VGI_STORAGE_S3_ACCESS_KEY_ID": _ACCESS_KEY,
        "VGI_STORAGE_S3_SECRET_ACCESS_KEY": _SECRET_KEY,
        "VGI_STORAGE_S3_VERIFY": "1",
        "VGI_STORAGE_ALLOW_PLAIN_HTTP": "1",
        **extra,
    }


def _objects_under(s3_client: object, prefix: str) -> int:
    response = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)  # type: ignore[attr-defined]
    return int(response.get("KeyCount", 0))


def _grant(port: int, payload: bytes) -> object:
    import hashlib

    from vgi_rpc.http import request_upload_urls

    return request_upload_urls(
        f"http://127.0.0.1:{port}", uploads=[(len(payload), hashlib.sha256(payload).hexdigest())]
    )[0]


def test_env_s3_results_land_under_prefix_rpc(
    s3_endpoint: str, s3_client: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VGI_STORAGE_S3_*: results externalize to <prefix>rpc/ and come back."""
    before = _objects_under(s3_client, "env/rpc/")
    big = "env-storage " * 4000
    with _env_worker(monkeypatch, _s3_env(s3_endpoint)) as port, _connect(port) as proxy:
        assert proxy.echo_large_string(value=big) == big
    assert _objects_under(s3_client, "env/rpc/") > before


def test_env_s3_grants_sign_the_checksum_header(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A grant signs x-amz-checksum-sha256 as a header (never an unsigned query
    parameter), so storage refuses a tampered body of the right length."""
    from urllib.parse import parse_qs, urlsplit

    import httpx2

    from vgi_rpc.http import upload_put_headers

    payload = b"direct storage grant " * 50
    with _env_worker(monkeypatch, _s3_env(s3_endpoint)) as port:
        grant = _grant(port, payload)
    query = {k.lower(): v for k, v in parse_qs(urlsplit(grant.upload_url).query).items()}
    assert "x-amz-checksum-sha256" in query["x-amz-signedheaders"][0].split(";")
    assert "x-amz-checksum-sha256" not in query
    tampered = bytes([payload[0] ^ 1]) + payload[1:]
    bad = httpx2.put(grant.upload_url, content=tampered, headers=upload_put_headers(grant), timeout=30)
    assert bad.status_code in (400, 403), bad.status_code
    good = httpx2.put(grant.upload_url, content=payload, headers=upload_put_headers(grant), timeout=30)
    assert good.status_code in (200, 204), good.text[:200]


def test_env_s3_checksum_none_issues_no_grants(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VGI_STORAGE_S3_CHECKSUM=none: results still externalize; no upload grants."""
    import httpx2

    from vgi_rpc.http import http_capabilities

    big = "checksum none " * 4000
    with _env_worker(monkeypatch, _s3_env(s3_endpoint, VGI_STORAGE_S3_CHECKSUM="none")) as port:
        caps = http_capabilities(f"http://127.0.0.1:{port}")
        assert not caps.upload_url_support
        head = httpx2.get(f"http://127.0.0.1:{port}/health", timeout=10)
        assert "vgi-upload-url-support" not in {k.lower() for k in head.headers}
        with _connect(port) as proxy:
            assert proxy.echo_large_string(value=big) == big
        with pytest.raises(Exception):  # noqa: B017, PT011 - 404: no grants here
            _grant(port, b"refused")


def test_env_s3_public_endpoint_signs_client_urls(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VGI_STORAGE_S3_PUBLIC_ENDPOINT goes into the URLs clients get; the worker
    still talks to VGI_STORAGE_S3_ENDPOINT."""
    from urllib.parse import urlsplit

    import httpx2

    from vgi_rpc.http import upload_put_headers

    port_number = urlsplit(s3_endpoint).port
    public = f"http://localhost:{port_number}"
    payload = b"public endpoint grant"
    env = _s3_env(s3_endpoint, VGI_STORAGE_S3_PUBLIC_ENDPOINT=public)
    with _env_worker(monkeypatch, env) as port:
        grant = _grant(port, payload)
        big = "public endpoint " * 4000
        with _connect(port) as proxy:
            assert proxy.echo_large_string(value=big) == big
    assert urlsplit(grant.upload_url).netloc == f"localhost:{port_number}"
    put = httpx2.put(grant.upload_url, content=payload, headers=upload_put_headers(grant), timeout=30)
    assert put.status_code in (200, 204), put.text[:200]


def test_env_s3_session_token_credentials(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temporary credentials from STS AssumeRole (RustFS validates the token)."""
    import boto3
    from urllib.parse import parse_qs, urlsplit

    sts = boto3.client(
        "sts",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
    )
    try:
        credentials = sts.assume_role(
            RoleArn="arn:aws:iam::000000000000:role/vgi-test",
            RoleSessionName="vgi-rpc-cpp",
            DurationSeconds=3600,
        )["Credentials"]
    except Exception as exc:  # noqa: BLE001
        _skip(f"the S3 server issues no STS session tokens ({type(exc).__name__})")
    env = _s3_env(
        s3_endpoint,
        VGI_STORAGE_S3_ACCESS_KEY_ID=credentials["AccessKeyId"],
        VGI_STORAGE_S3_SECRET_ACCESS_KEY=credentials["SecretAccessKey"],
        VGI_STORAGE_S3_SESSION_TOKEN=credentials["SessionToken"],
        VGI_STORAGE_S3_SESSION_EXPIRES=credentials["Expiration"].isoformat(),
    )
    payload = b"session token grant"
    big = "session token " * 4000
    with _env_worker(monkeypatch, env) as port:
        with _connect(port) as proxy:
            assert proxy.echo_large_string(value=big) == big
        grant = _grant(port, payload)
    query = {k.lower(): v for k, v in parse_qs(urlsplit(grant.upload_url).query).items()}
    assert "x-amz-security-token" in query
    assert int(query["x-amz-expires"][0]) <= 3600 - 60

    # A token the server does not know fails the startup check, not a request.
    env["VGI_STORAGE_S3_SESSION_TOKEN"] = "not-a-real-token"
    with pytest.raises(RuntimeError):
        with _env_worker(monkeypatch, env):
            pass


def test_env_s3_verify_reports_a_missing_bucket(s3_capable_worker: str, s3_endpoint: str) -> None:
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith(("VGI_STORAGE_", "VGI_BLOB_VENDOR"))},
        **_s3_env(s3_endpoint, VGI_STORAGE_S3_BUCKET="no-such-bucket-vgi"),
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    proc = subprocess.run(
        [s3_capable_worker, "--http", "--storage-from-env"],
        capture_output=True,
        timeout=60,
        check=False,
        env=env,
    )
    assert proc.returncode != 0
    assert b"does not exist" in proc.stderr, proc.stderr[-400:]


# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gcs_service_account(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Write a throwaway service-account key, returning its path.

    Never used against Google: V4 signing is a local RSA operation, so a key
    generated here signs exactly as a real one would.
    """
    import json

    openssl = shutil.which("openssl")
    if not openssl:
        _skip("openssl is needed to generate a signing key")

    tmp = tmp_path_factory.mktemp("gcs")
    pem = tmp / "key.pem"
    subprocess.run([openssl, "genrsa", "-out", str(pem), "2048"],
                   capture_output=True, check=True, timeout=60)
    path = tmp / "service-account.json"
    path.write_text(json.dumps({
        "type": "service_account",
        "project_id": "vgi-rpc-test",
        "private_key_id": "test",
        "private_key": pem.read_text(),
        "client_email": "vgi-rpc-test@vgi-rpc-test.iam.gserviceaccount.com",
        "client_id": "0",
        "token_uri": "https://oauth2.googleapis.com/token",
    }))
    return str(path)


@pytest.fixture(scope="module")
def gcs_worker(gcs_service_account: str) -> Iterator[int]:
    """A worker configured for GCS with the throwaway key, yielding its port."""
    saved_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    saved_emulator = os.environ.pop("CLOUD_STORAGE_EMULATOR_ENDPOINT", None)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcs_service_account
    try:
        with spawn_http(
            "--external-storage", "gs://vgi-bucket/vgi-rpc",
            "--externalize-threshold", "4096",
        ) as port:
            # A build without the backend refuses the URI at startup, so
            # spawn_http would not have reached here.
            yield port
    except RuntimeError as exc:
        _skip(f"worker could not serve gs:// ({exc})")
    finally:
        if saved_creds is None:
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        else:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = saved_creds
        if saved_emulator is not None:
            os.environ["CLOUD_STORAGE_EMULATOR_ENDPOINT"] = saved_emulator


def test_gcs_vends_v4_signed_urls(gcs_worker: int) -> None:
    """Vended URLs must be genuine GOOG4 signatures over the right object.

    Everything asserted here is visible in the URL itself, which is the point:
    a client fetches it with no credentials, so if the query string is wrong
    the object is unreachable and nothing else in the feature works.
    """
    import hashlib

    from vgi_rpc.http import request_upload_urls

    # Upload URLs v2: one (size, sha256) entry per URL requested.
    bodies = (b"first upload", b"second upload")
    urls = request_upload_urls(
        f"http://127.0.0.1:{gcs_worker}",
        uploads=[(len(b), hashlib.sha256(b).hexdigest()) for b in bodies],
    )
    assert len(urls) == 2

    for pair in urls:
        for url in (pair.upload_url, pair.download_url):
            assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in url, f"not a V4 signature: {url[:120]}"
            assert "X-Goog-Signature=" in url
            assert "vgi-rpc-test%40vgi-rpc-test.iam.gserviceaccount.com" in url, (
                "signed by an unexpected account"
            )
            assert "/vgi-bucket/vgi-rpc/" in url, "signed over the wrong bucket or prefix"
        assert pair.upload_url != pair.download_url, (
            "a pre-signing backend must vend one credential per verb"
        )

    assert urls[0].upload_url != urls[1].upload_url, "each pair must name its own object"
    # Protocol 3.4: each grant names the key its PUT creates, under the prefix,
    # and carries a sealed argument reference to it.
    for pair in urls:
        assert pair.object_id.startswith("vgi-rpc/"), f"object_id is not the key: {pair.object_id!r}"
        assert f"/vgi-bucket/{pair.object_id}" in pair.upload_url
        assert pair.ref
