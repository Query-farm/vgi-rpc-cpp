"""Quick conformance test against the C++ worker."""
import os
import sys
_vgi_rpc_path = os.environ.get(
    "VGI_RPC_PYTHON_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "vgi-rpc"),
)
sys.path.insert(0, _vgi_rpc_path)

from vgi_rpc.rpc import SubprocessTransport, RpcConnection, AnnotatedBatch, RpcError
from vgi_rpc.conformance import ConformanceService, Point, BoundingBox, Status, AllTypes, ConformanceHeader
from vgi_rpc.introspect import introspect
from vgi_rpc.log import Level, Message
import pyarrow as pa

CMD = [os.environ.get(
    "CONFORMANCE_WORKER",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "build/conformance/conformance_worker"),
)]

logs: list[Message] = []

def on_log(msg: Message):
    logs.append(msg)

def clear_logs():
    logs.clear()

transport = SubprocessTransport(CMD)

with RpcConnection(ConformanceService, transport, on_log=on_log) as conn:
    print("=== Scalar Echo ===")
    assert conn.echo_string(value="hello") == "hello"
    assert conn.echo_int(value=42) == 42
    assert conn.echo_float(value=3.14) == 3.14
    assert conn.echo_bool(value=True) is True
    assert conn.echo_bytes(data=b"hello") == b"hello"
    print("  PASS")

    print("=== Void ===")
    assert conn.void_noop() is None
    assert conn.void_with_param(value=99) is None
    print("  PASS")

    print("=== Complex Types ===")
    assert conn.echo_enum(status=Status.ACTIVE) == Status.ACTIVE
    assert conn.echo_list(values=["a", "b", "c"]) == ["a", "b", "c"]
    assert conn.echo_dict(mapping={"x": 1, "y": 2}) == {"x": 1, "y": 2}
    assert conn.echo_nested_list(matrix=[[1, 2], [3, 4]]) == [[1, 2], [3, 4]]
    print("  PASS")

    print("=== Optional ===")
    assert conn.echo_optional_string(value="hi") == "hi"
    assert conn.echo_optional_string(value=None) is None
    assert conn.echo_optional_int(value=5) == 5
    assert conn.echo_optional_int(value=None) is None
    print("  PASS")

    print("=== Dataclass ===")
    p = Point(x=1.0, y=2.0)
    assert conn.echo_point(point=p) == p
    result = conn.inspect_point(point=p)
    assert result == "Point(1.0, 2.0)", f"inspect_point got: {result!r}"
    bb = BoundingBox(top_left=Point(x=0.0, y=10.0), bottom_right=Point(x=10.0, y=0.0), label="test")
    assert conn.echo_bounding_box(box=bb) == bb
    print("  PASS")

    print("=== Annotated Types ===")
    assert conn.echo_int32(value=123) == 123
    assert conn.echo_float32(value=1.5) == 1.5
    print("  PASS")

    print("=== Multi-Param ===")
    assert conn.add_floats(a=1.5, b=2.5) == 4.0
    assert conn.concatenate(prefix="hello", suffix="world", separator="_") == "hello_world"
    assert conn.with_defaults(required=10, optional_str="custom", optional_int=99) == "required=10, optional_str=custom, optional_int=99"
    print("  PASS")

    print("=== Error Propagation ===")
    try:
        conn.raise_value_error(message="bad value")
        assert False
    except RpcError as e:
        assert e.error_type == "ValueError", f"got {e.error_type}"
        assert e.error_message == "bad value"

    try:
        conn.raise_runtime_error(message="runtime")
        assert False
    except RpcError as e:
        assert e.error_type == "RuntimeError"

    try:
        conn.raise_type_error(message="wrong type")
        assert False
    except RpcError as e:
        assert e.error_type == "TypeError"
    print("  PASS")

    print("=== Logging ===")
    clear_logs()
    assert conn.echo_with_info_log(value="test") == "test"
    assert len(logs) == 1
    assert logs[0].level == Level.INFO

    clear_logs()
    assert conn.echo_with_multi_logs(value="test") == "test"
    assert len(logs) == 3
    assert [l.level for l in logs] == [Level.DEBUG, Level.INFO, Level.WARN]

    clear_logs()
    assert conn.echo_with_log_extras(value="test") == "test"
    assert len(logs) == 1
    assert logs[0].extra.get("source") == "conformance"
    assert logs[0].extra.get("detail") == "test"
    print("  PASS")

    print("=== Producer Streams ===")
    results = list(conn.produce_n(count=3))
    assert len(results) == 3
    for i, ab in enumerate(results):
        assert ab.batch.column("index")[0].as_py() == i
        assert ab.batch.column("value")[0].as_py() == i * 10

    assert len(list(conn.produce_empty())) == 0
    assert len(list(conn.produce_single())) == 1

    results = list(conn.produce_large_batches(rows_per_batch=100, batch_count=3))
    assert len(results) == 3
    assert results[0].batch.num_rows == 100
    print("  PASS")

    print("=== Producer with Logs ===")
    clear_logs()
    results = list(conn.produce_with_logs(count=2))
    assert len(results) == 2
    assert len(logs) == 2
    assert all(l.level == Level.INFO for l in logs)
    print("  PASS")

    print("=== Producer Errors ===")
    try:
        list(conn.produce_error_mid_stream(emit_before_error=2))
        assert False
    except RpcError as e:
        assert "intentional error" in e.error_message

    try:
        list(conn.produce_error_on_init())
        assert False
    except RpcError as e:
        assert "intentional init error" in e.error_message
    print("  PASS")

    print("=== Producer with Headers ===")
    with conn.produce_with_header(count=2) as session:
        header = session.typed_header(ConformanceHeader)
        assert header.total_expected == 2
        assert "producing 2 batches" in header.description
        results = list(session)
        assert len(results) == 2

    clear_logs()
    with conn.produce_with_header_and_logs(count=2) as session:
        header = session.typed_header(ConformanceHeader)
        assert header.total_expected == 2
        results = list(session)
        assert len(results) == 2
    assert any(l.level == Level.INFO for l in logs)
    print("  PASS")

    print("=== Exchange Streams ===")
    with conn.exchange_scale(factor=2.5) as session:
        result = session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([10.0, 20.0])], names=["value"])))
        assert result.batch.column("value").to_pylist() == [25.0, 50.0]

    with conn.exchange_accumulate() as session:
        r1 = session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([5.0])], names=["value"])))
        assert r1.batch.column("running_sum")[0].as_py() == 5.0
        assert r1.batch.column("exchange_count")[0].as_py() == 1
        r2 = session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([3.0])], names=["value"])))
        assert r2.batch.column("running_sum")[0].as_py() == 8.0
        assert r2.batch.column("exchange_count")[0].as_py() == 2

    clear_logs()
    with conn.exchange_with_logs() as session:
        result = session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([7.0])], names=["value"])))
        assert result.batch.column("value")[0].as_py() == 7.0
    assert len(logs) == 2  # INFO + DEBUG
    print("  PASS")

    print("=== Exchange Errors ===")
    try:
        with conn.exchange_error_on_nth(fail_on=2) as session:
            session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([1.0])], names=["value"])))
            session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([2.0])], names=["value"])))
            assert False
    except RpcError as e:
        assert "intentional error" in e.error_message

    # exchange_error_on_init: factory throws before any state is created.
    # On subprocess transport, we must actually send data to trigger reading the error
    # (the server drains the client's input IPC stream after writing the init error).
    try:
        with conn.exchange_error_on_init() as session:
            session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([1.0])], names=["value"])))
            assert False
    except RpcError as e:
        assert "intentional exchange init error" in e.error_message
    print("  PASS")

    print("=== Exchange with Header ===")
    with conn.exchange_with_header(factor=3.0) as session:
        header = session.typed_header(ConformanceHeader)
        assert header.total_expected == 0
        result = session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([10.0])], names=["value"])))
        assert result.batch.column("value")[0].as_py() == 30.0
    print("  PASS")

    print("  PASS")

print("=== Describe Introspection ===")
transport2 = SubprocessTransport(CMD)
try:
    desc = introspect(transport2)
    method_names = list(desc.methods.keys())
    assert "echo_string" in method_names, f"echo_string not found in {method_names}"
    assert "__describe__" not in method_names, "__describe__ should not be in listing"
    print("  PASS")
finally:
    transport2.close()

print("\n=== ALL CONFORMANCE TESTS PASSED ===")
