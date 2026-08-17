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
* an S3-compatible server — MinIO under Docker, or a `moto` server if
  ``moto[server]`` happens to be installed.

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
        pytest.skip("worker was built without the S3 backend")
    pytest.skip(f"S3 probe worker exited unexpectedly: {stderr.decode()[:200]}")


@pytest.fixture(scope="module")
def s3_endpoint(s3_capable_worker: str) -> Iterator[str]:
    """A local S3-compatible server with the bucket created, yielding its URL."""
    boto3 = pytest.importorskip("boto3", reason="boto3 is not installed")

    port = _free_port()
    proc: subprocess.Popen[bytes] | None = None
    container = ""

    if shutil.which("docker"):
        container = f"vgi-rpc-minio-{port}"
        proc = subprocess.Popen(
            ["docker", "run", "--rm", "--name", container,
             "-p", f"127.0.0.1:{port}:9000",
             "-e", f"MINIO_ROOT_USER={_ACCESS_KEY}",
             "-e", f"MINIO_ROOT_PASSWORD={_SECRET_KEY}",
             "minio/minio:latest", "server", "/data"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        pytest.importorskip("flask", reason="neither Docker nor moto[server] is available")
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
        deadline = time.monotonic() + 60
        while True:
            try:
                client.create_bucket(Bucket=_BUCKET)
                break
            except Exception as exc:  # noqa: BLE001, PERF203
                if "BucketAlreadyOwnedByYou" in str(exc):
                    break
                if time.monotonic() > deadline:
                    pytest.skip(f"no S3-compatible server became ready: {exc}")
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
    import httpx2

    from vgi_rpc.http import request_upload_urls

    urls = request_upload_urls(f"http://127.0.0.1:{s3_worker}", count=2)
    assert len(urls) == 2
    assert urls[0].upload_url != urls[0].download_url, (
        "a pre-signing backend must vend one credential per verb"
    )

    payload = b"client-vended upload contents"
    put = httpx2.put(urls[0].upload_url, content=payload, timeout=30.0)
    assert put.status_code in (200, 204), f"PUT failed: {put.status_code} {put.text[:200]}"

    got = httpx2.get(urls[0].download_url, timeout=30.0)
    assert got.status_code == 200
    assert got.content == payload


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
        pytest.skip("openssl is needed to generate a signing key")

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
        pytest.skip(f"worker could not serve gs:// ({exc})")
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
    from vgi_rpc.http import request_upload_urls

    urls = request_upload_urls(f"http://127.0.0.1:{gcs_worker}", count=2)
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
