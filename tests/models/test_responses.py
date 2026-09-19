import json
from unittest import mock

import brotli
import pytest

import httpx
from httpx._content_streams import AsyncIteratorStream, IteratorStream


def streaming_body():
    yield b"Hello, "
    yield b"world!"


async def async_streaming_body():
    yield b"Hello, "
    yield b"world!"


def empty_streaming_body():
    return
    yield  # pragma: no cover


async def empty_async_streaming_body():
    return
    yield  # pragma: no cover


def chunked_streaming_body():
    yield b"Hel"
    yield b"lo, "
    yield b"wor"
    yield b"ld!"


async def chunked_async_streaming_body():
    yield b"Hel"
    yield b"lo, "
    yield b"wor"
    yield b"ld!"


class ConsumedStream:
    """
    An iterable that records how many times it was iterated, so we can
    verify a streaming response is only ever consumed once.
    """

    def __init__(self):
        self.iteration_count = 0

    def __iter__(self):
        self.iteration_count += 1
        for chunk in (b"Hello, ", b"world!"):
            yield chunk


class AsyncConsumedStream:
    def __init__(self):
        self.iteration_count = 0

    async def __aiter__(self):
        self.iteration_count += 1
        for chunk in (b"Hello, ", b"world!"):
            yield chunk


def test_response():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
        request=httpx.Request("GET", "https://example.org"),
    )

    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.text == "Hello, world!"
    assert response.request.method == "GET"
    assert response.request.url == "https://example.org"
    assert not response.is_error


def test_raise_for_status():
    request = httpx.Request("GET", "https://example.org")

    # 2xx status codes are not an error.
    response = httpx.Response(200, request=request)
    response.raise_for_status()

    # 4xx status codes are a client error.
    response = httpx.Response(403, request=request)
    with pytest.raises(httpx.HTTPStatusError):
        response.raise_for_status()

    # 5xx status codes are a server error.
    response = httpx.Response(500, request=request)
    with pytest.raises(httpx.HTTPStatusError):
        response.raise_for_status()

    # Calling .raise_for_status without setting a request instance is
    # not valid. Should raise a runtime error.
    response = httpx.Response(200)
    with pytest.raises(RuntimeError):
        response.raise_for_status()


def test_response_repr():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )
    assert repr(response) == "<Response [200 OK]>"


def test_response_content_type_encoding():
    """
    Use the charset encoding in the Content-Type header if possible.
    """
    headers = {"Content-Type": "text-plain; charset=latin-1"}
    content = "Latin 1: ÿ".encode("latin-1")
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.text == "Latin 1: ÿ"
    assert response.encoding == "latin-1"


def test_response_autodetect_encoding():
    """
    Autodetect encoding if there is no charset info in a Content-Type header.
    """
    content = "おはようございます。".encode("EUC-JP")
    response = httpx.Response(
        200,
        content=content,
    )
    assert response.text == "おはようございます。"
    assert response.encoding == "EUC-JP"


def test_response_fallback_to_autodetect():
    """
    Fallback to autodetection if we get an invalid charset in the Content-Type header.
    """
    headers = {"Content-Type": "text-plain; charset=invalid-codec-name"}
    content = "おはようございます。".encode("EUC-JP")
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.text == "おはようございます。"
    assert response.encoding == "EUC-JP"


def test_response_default_text_encoding():
    """
    A media type of 'text/*' with no charset should default to ISO-8859-1.
    See: https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.7.1
    """
    content = b"Hello, world!"
    headers = {"Content-Type": "text/plain"}
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.status_code == 200
    assert response.encoding == "iso-8859-1"
    assert response.text == "Hello, world!"


def test_response_default_encoding():
    """
    Default to utf-8 if all else fails.
    """
    response = httpx.Response(
        200,
        content=b"",
    )
    assert response.text == ""
    assert response.encoding == "utf-8"


def test_response_non_text_encoding():
    """
    Default to apparent encoding for non-text content-type headers.
    """
    headers = {"Content-Type": "image/png"}
    response = httpx.Response(
        200,
        content=b"xyz",
        headers=headers,
    )
    assert response.text == "xyz"
    assert response.encoding == "ascii"


def test_response_set_explicit_encoding():
    headers = {
        "Content-Type": "text-plain; charset=utf-8"
    }  # Deliberately incorrect charset
    response = httpx.Response(
        200,
        content="Latin 1: ÿ".encode("latin-1"),
        headers=headers,
    )
    response.encoding = "latin-1"
    assert response.text == "Latin 1: ÿ"
    assert response.encoding == "latin-1"


def test_response_force_encoding():
    response = httpx.Response(
        200,
        content="Snowman: ☃".encode("utf-8"),
    )
    response.encoding = "iso-8859-1"
    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.text == "Snowman: â\x98\x83"
    assert response.encoding == "iso-8859-1"


def test_read():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    assert response.status_code == 200
    assert response.text == "Hello, world!"
    assert response.encoding == "ascii"
    assert response.is_closed

    content = response.read()

    assert content == b"Hello, world!"
    assert response.content == b"Hello, world!"
    assert response.is_closed


@pytest.mark.asyncio
async def test_aread():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    assert response.status_code == 200
    assert response.text == "Hello, world!"
    assert response.encoding == "ascii"
    assert response.is_closed

    content = await response.aread()

    assert content == b"Hello, world!"
    assert response.content == b"Hello, world!"
    assert response.is_closed


def test_iter_raw():
    stream = IteratorStream(iterator=streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    raw = b""
    for part in response.iter_raw():
        raw += part
    assert raw == b"Hello, world!"


def test_iter_raw_increments_updates_counter():
    stream = IteratorStream(iterator=streaming_body())

    response = httpx.Response(
        200,
        stream=stream,
    )

    num_downloaded = response.num_bytes_downloaded
    for part in response.iter_raw():
        assert len(part) == (response.num_bytes_downloaded - num_downloaded)
        num_downloaded = response.num_bytes_downloaded


@pytest.mark.asyncio
async def test_aiter_raw():
    stream = AsyncIteratorStream(aiterator=async_streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    raw = b""
    async for part in response.aiter_raw():
        raw += part
    assert raw == b"Hello, world!"


@pytest.mark.asyncio
async def test_aiter_raw_increments_updates_counter():
    stream = AsyncIteratorStream(aiterator=async_streaming_body())

    response = httpx.Response(
        200,
        stream=stream,
    )

    num_downloaded = response.num_bytes_downloaded
    async for part in response.aiter_raw():
        assert len(part) == (response.num_bytes_downloaded - num_downloaded)
        num_downloaded = response.num_bytes_downloaded


def test_iter_bytes():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    content = b""
    for part in response.iter_bytes():
        content += part
    assert content == b"Hello, world!"


@pytest.mark.asyncio
async def test_aiter_bytes():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    content = b""
    async for part in response.aiter_bytes():
        content += part
    assert content == b"Hello, world!"


def test_iter_text():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    content = ""
    for part in response.iter_text():
        content += part
    assert content == "Hello, world!"


@pytest.mark.asyncio
async def test_aiter_text():
    response = httpx.Response(
        200,
        content=b"Hello, world!",
    )

    content = ""
    async for part in response.aiter_text():
        content += part
    assert content == "Hello, world!"


def test_iter_lines():
    response = httpx.Response(
        200,
        content=b"Hello,\nworld!",
    )

    content = []
    for line in response.iter_lines():
        content.append(line)
    assert content == ["Hello,\n", "world!"]


@pytest.mark.asyncio
async def test_aiter_lines():
    response = httpx.Response(
        200,
        content=b"Hello,\nworld!",
    )

    content = []
    async for line in response.aiter_lines():
        content.append(line)
    assert content == ["Hello,\n", "world!"]


def test_sync_streaming_response():
    stream = IteratorStream(iterator=streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    assert response.status_code == 200
    assert not response.is_closed

    content = response.read()

    assert content == b"Hello, world!"
    assert response.content == b"Hello, world!"
    assert response.is_closed


@pytest.mark.asyncio
async def test_async_streaming_response():
    stream = AsyncIteratorStream(aiterator=async_streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    assert response.status_code == 200
    assert not response.is_closed

    content = await response.aread()

    assert content == b"Hello, world!"
    assert response.content == b"Hello, world!"
    assert response.is_closed


def test_cannot_read_after_stream_consumed():
    stream = IteratorStream(iterator=streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    content = b""
    for part in response.iter_bytes():
        content += part

    with pytest.raises(httpx.StreamConsumed):
        response.read()


@pytest.mark.asyncio
async def test_cannot_aread_after_stream_consumed():
    stream = AsyncIteratorStream(aiterator=async_streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    content = b""
    async for part in response.aiter_bytes():
        content += part

    with pytest.raises(httpx.StreamConsumed):
        await response.aread()


def test_cannot_read_after_response_closed():
    is_closed = False

    def close_func():
        nonlocal is_closed
        is_closed = True

    stream = IteratorStream(iterator=streaming_body(), close_func=close_func)
    response = httpx.Response(
        200,
        stream=stream,
    )

    response.close()
    assert is_closed

    with pytest.raises(httpx.ResponseClosed):
        response.read()


@pytest.mark.asyncio
async def test_cannot_aread_after_response_closed():
    is_closed = False

    async def close_func():
        nonlocal is_closed
        is_closed = True

    stream = AsyncIteratorStream(
        aiterator=async_streaming_body(), close_func=close_func
    )
    response = httpx.Response(
        200,
        stream=stream,
    )

    await response.aclose()
    assert is_closed

    with pytest.raises(httpx.ResponseClosed):
        await response.aread()


@pytest.mark.asyncio
async def test_elapsed_not_available_until_closed():
    stream = AsyncIteratorStream(aiterator=async_streaming_body())
    response = httpx.Response(
        200,
        stream=stream,
    )

    with pytest.raises(RuntimeError):
        response.elapsed


def test_unknown_status_code():
    response = httpx.Response(
        600,
    )
    assert response.status_code == 600
    assert response.reason_phrase == ""
    assert response.text == ""


def test_json_with_specified_encoding():
    data = {"greeting": "hello", "recipient": "world"}
    content = json.dumps(data).encode("utf-16")
    headers = {"Content-Type": "application/json, charset=utf-16"}
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.json() == data


def test_json_with_options():
    data = {"greeting": "hello", "recipient": "world", "amount": 1}
    content = json.dumps(data).encode("utf-16")
    headers = {"Content-Type": "application/json, charset=utf-16"}
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.json(parse_int=str)["amount"] == "1"


def test_json_without_specified_encoding():
    data = {"greeting": "hello", "recipient": "world"}
    content = json.dumps(data).encode("utf-32-be")
    headers = {"Content-Type": "application/json"}
    response = httpx.Response(
        200,
        content=content,
        headers=headers,
    )
    assert response.json() == data


def test_json_without_specified_encoding_decode_error():
    data = {"greeting": "hello", "recipient": "world"}
    content = json.dumps(data).encode("utf-32-be")
    headers = {"Content-Type": "application/json"}
    # force incorrect guess from `guess_json_utf` to trigger error
    with mock.patch("httpx._models.guess_json_utf", return_value="utf-32"):
        response = httpx.Response(
            200,
            content=content,
            headers=headers,
        )
        with pytest.raises(json.decoder.JSONDecodeError):
            response.json()


def test_json_without_specified_encoding_value_error():
    data = {"greeting": "hello", "recipient": "world"}
    content = json.dumps(data).encode("utf-32-be")
    headers = {"Content-Type": "application/json"}
    # force incorrect guess from `guess_json_utf` to trigger error
    with mock.patch("httpx._models.guess_json_utf", return_value="utf-32"):
        response = httpx.Response(200, content=content, headers=headers)
        with pytest.raises(ValueError):
            response.json()


@pytest.mark.parametrize(
    "headers, expected",
    [
        (
            {"Link": "<https://example.com>; rel='preload'"},
            {"preload": {"rel": "preload", "url": "https://example.com"}},
        ),
        (
            {"Link": '</hub>; rel="hub", </resource>; rel="self"'},
            {
                "hub": {"url": "/hub", "rel": "hub"},
                "self": {"url": "/resource", "rel": "self"},
            },
        ),
    ],
)
def test_link_headers(headers, expected):
    response = httpx.Response(
        200,
        content=None,
        headers=headers,
    )
    assert response.links == expected


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br"))
def test_decode_error_with_request(header_value):
    headers = [(b"Content-Encoding", header_value)]
    body = b"test 123"
    compressed_body = brotli.compress(body)[3:]
    with pytest.raises(ValueError):
        httpx.Response(
            200,
            headers=headers,
            content=compressed_body,
        )

    with pytest.raises(httpx.DecodingError):
        httpx.Response(
            200,
            headers=headers,
            content=compressed_body,
            request=httpx.Request("GET", "https://www.example.org/"),
        )


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br"))
def test_value_error_without_request(header_value):
    headers = [(b"Content-Encoding", header_value)]
    body = b"test 123"
    compressed_body = brotli.compress(body)[3:]
    with pytest.raises(ValueError):
        httpx.Response(200, headers=headers, content=compressed_body)


def test_response_with_unset_request():
    response = httpx.Response(200, content=b"Hello, world!")

    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.text == "Hello, world!"
    assert not response.is_error


def test_set_request_after_init():
    response = httpx.Response(200, content=b"Hello, world!")

    response.request = httpx.Request("GET", "https://www.example.org")

    assert response.request.method == "GET"
    assert response.request.url == "https://www.example.org"


def test_cannot_access_unset_request():
    response = httpx.Response(200, content=b"Hello, world!")

    with pytest.raises(RuntimeError):
        response.request


class TestContentFromIterator:
    def test_bytes_content_sets_content_length(self):
        response = httpx.Response(200, content=b"Hello, world!")

        assert response.headers["Content-Length"] == "13"
        assert "Transfer-Encoding" not in response.headers
        # Known length content is read eagerly.
        assert response.is_closed
        assert response.content == b"Hello, world!"

    def test_empty_bytes_content_does_not_set_content_length(self):
        response = httpx.Response(200, content=b"")

        assert "Content-Length" not in response.headers
        assert response.content == b""
        assert response.is_closed

    def test_str_content_sets_content_length(self):
        response = httpx.Response(200, content="Hello, world!")

        assert response.headers["Content-Length"] == "13"
        assert response.content == b"Hello, world!"
        assert response.text == "Hello, world!"

    def test_iterator_content_does_not_set_content_length(self):
        response = httpx.Response(200, content=streaming_body())

        assert "Content-Length" not in response.headers
        assert response.headers["Transfer-Encoding"] == "chunked"
        # Streaming content is not read eagerly.
        assert not response.is_closed
        with pytest.raises(httpx.ResponseNotRead):
            response.content

    def test_explicit_content_length_is_not_overridden_by_chunked(self):
        response = httpx.Response(
            200,
            headers={"Content-Length": "13"},
            content=streaming_body(),
        )

        assert response.headers["Content-Length"] == "13"
        assert "Transfer-Encoding" not in response.headers

    def test_async_iterator_content_does_not_set_content_length(self):
        response = httpx.Response(200, content=async_streaming_body())

        assert "Content-Length" not in response.headers
        assert response.headers["Transfer-Encoding"] == "chunked"
        assert not response.is_closed
        with pytest.raises(httpx.ResponseNotRead):
            response.content

    def test_sync_iterator_read(self):
        response = httpx.Response(200, content=streaming_body())

        assert response.read() == b"Hello, world!"
        assert response.content == b"Hello, world!"
        assert response.is_closed

    def test_sync_iterator_iter_bytes(self):
        response = httpx.Response(200, content=chunked_streaming_body())

        chunks = list(response.iter_bytes())
        assert b"".join(chunks) == b"Hello, world!"
        # The chunk boundaries of the underlying stream are preserved.
        assert [chunk for chunk in chunks if chunk] == [
            b"Hel",
            b"lo, ",
            b"wor",
            b"ld!",
        ]
        assert response.is_closed

    def test_sync_iterator_iter_raw(self):
        response = httpx.Response(200, content=chunked_streaming_body())

        chunks = []
        for part in response.iter_raw():
            chunks.append(part)
        assert chunks == [b"Hel", b"lo, ", b"wor", b"ld!"]
        assert response.is_closed

    def test_empty_sync_iterator(self):
        response = httpx.Response(200, content=empty_streaming_body())

        assert response.read() == b""
        assert response.content == b""
        assert response.is_closed

    def test_sync_iterable_is_wrapped_and_consumed_once(self):
        stream = ConsumedStream()
        response = httpx.Response(200, content=stream)

        assert response.read() == b"Hello, world!"
        assert stream.iteration_count == 1
        assert response.is_closed

        # The underlying iterable is never consumed a second time.
        with pytest.raises(httpx.StreamConsumed):
            list(response.iter_raw())
        assert stream.iteration_count == 1

    @pytest.mark.asyncio
    async def test_async_iterator_aread(self):
        response = httpx.Response(200, content=async_streaming_body())

        assert await response.aread() == b"Hello, world!"
        assert response.content == b"Hello, world!"
        assert response.is_closed

    @pytest.mark.asyncio
    async def test_async_iterator_aiter_bytes(self):
        response = httpx.Response(200, content=chunked_async_streaming_body())

        chunks = [part async for part in response.aiter_bytes()]
        assert b"".join(chunks) == b"Hello, world!"
        assert [chunk for chunk in chunks if chunk] == [
            b"Hel",
            b"lo, ",
            b"wor",
            b"ld!",
        ]
        assert response.is_closed

    @pytest.mark.asyncio
    async def test_async_iterator_aiter_raw(self):
        response = httpx.Response(200, content=chunked_async_streaming_body())

        chunks = [part async for part in response.aiter_raw()]
        assert chunks == [b"Hel", b"lo, ", b"wor", b"ld!"]
        assert response.is_closed

    @pytest.mark.asyncio
    async def test_empty_async_iterator(self):
        response = httpx.Response(200, content=empty_async_streaming_body())

        assert await response.aread() == b""
        assert response.content == b""
        assert response.is_closed

    @pytest.mark.asyncio
    async def test_async_iterable_is_consumed_once(self):
        stream = AsyncConsumedStream()
        response = httpx.Response(200, content=stream)

        assert await response.aread() == b"Hello, world!"
        assert stream.iteration_count == 1

        with pytest.raises(httpx.StreamConsumed):
            [part async for part in response.aiter_raw()]
        assert stream.iteration_count == 1

    def test_repeated_sync_read_returns_cached_content(self):
        response = httpx.Response(200, content=streaming_body())

        first = response.read()
        second = response.read()
        assert first == second == b"Hello, world!"

    @pytest.mark.asyncio
    async def test_repeated_async_aread_returns_cached_content(self):
        response = httpx.Response(200, content=async_streaming_body())

        first = await response.aread()
        second = await response.aread()
        assert first == second == b"Hello, world!"

    def test_cannot_sync_read_after_sync_streaming_consumed(self):
        response = httpx.Response(200, content=streaming_body())

        assert b"".join(response.iter_bytes()) == b"Hello, world!"

        with pytest.raises(httpx.StreamConsumed):
            response.read()

    @pytest.mark.asyncio
    async def test_cannot_async_aread_after_async_streaming_consumed(self):
        response = httpx.Response(200, content=async_streaming_body())

        chunks = [part async for part in response.aiter_bytes()]
        assert b"".join(chunks) == b"Hello, world!"

        with pytest.raises(httpx.StreamConsumed):
            await response.aread()

    def test_cannot_sync_consume_async_iterator(self):
        response = httpx.Response(200, content=async_streaming_body())

        with pytest.raises(RuntimeError):
            response.read()

    def test_cannot_sync_iter_raw_on_async_iterator(self):
        response = httpx.Response(200, content=async_streaming_body())

        with pytest.raises(RuntimeError):
            list(response.iter_raw())

    @pytest.mark.asyncio
    async def test_cannot_async_consume_sync_iterator(self):
        response = httpx.Response(200, content=streaming_body())

        with pytest.raises(RuntimeError):
            await response.aread()

    @pytest.mark.asyncio
    async def test_cannot_async_aiter_raw_on_sync_iterator(self):
        response = httpx.Response(200, content=streaming_body())

        with pytest.raises(RuntimeError):
            [part async for part in response.aiter_raw()]

    def test_sync_iterator_close_before_read(self):
        is_closed = False

        def close_func():
            nonlocal is_closed
            is_closed = True

        stream = IteratorStream(iterator=streaming_body(), close_func=close_func)
        response = httpx.Response(200, stream=stream)

        response.close()
        assert is_closed

        with pytest.raises(httpx.ResponseClosed):
            response.read()
        with pytest.raises(httpx.ResponseClosed):
            list(response.iter_raw())

    @pytest.mark.asyncio
    async def test_async_iterator_aclose_before_aread(self):
        is_closed = False

        async def close_func():
            nonlocal is_closed
            is_closed = True

        stream = AsyncIteratorStream(
            aiterator=async_streaming_body(), close_func=close_func
        )
        response = httpx.Response(200, stream=stream)

        await response.aclose()
        assert is_closed

        with pytest.raises(httpx.ResponseClosed):
            await response.aread()
        with pytest.raises(httpx.ResponseClosed):
            [part async for part in response.aiter_raw()]

    def test_abandoning_sync_iterator_closes_response(self):
        is_closed = False

        def close_func():
            nonlocal is_closed
            is_closed = True

        stream = IteratorStream(iterator=streaming_body(), close_func=close_func)
        response = httpx.Response(200, stream=stream)

        iterator = response.iter_raw()
        assert next(iterator) == b"Hello, "
        assert not is_closed

        # Abandoning iteration partway through still closes the response.
        del iterator
        assert is_closed
        assert response.is_closed
        assert response.is_stream_consumed

    @pytest.mark.asyncio
    async def test_abandoning_async_iterator_closes_response(self):
        is_closed = False

        async def close_func():
            nonlocal is_closed
            is_closed = True

        stream = AsyncIteratorStream(
            aiterator=async_streaming_body(), close_func=close_func
        )
        response = httpx.Response(200, stream=stream)

        aiterator = response.aiter_raw().__aiter__()
        assert await aiterator.__anext__() == b"Hello, "
        assert not is_closed

        await aiterator.aclose()
        assert is_closed
        assert response.is_closed
        assert response.is_stream_consumed

    def test_sync_generator_exception_propagates(self):
        def failing_body():
            yield b"Hello, "
            raise ValueError("boom")

        response = httpx.Response(200, content=failing_body())

        with pytest.raises(ValueError, match="boom"):
            response.read()

        # The response is left in a consistent closed/consumed state.
        assert response.is_closed
        assert response.is_stream_consumed
        with pytest.raises(httpx.StreamConsumed):
            list(response.iter_raw())

    def test_sync_generator_exception_partway_through_iter(self):
        def failing_body():
            yield b"Hello, "
            raise ValueError("boom")

        response = httpx.Response(200, content=failing_body())

        chunks = []
        with pytest.raises(ValueError, match="boom"):
            for part in response.iter_raw():
                chunks.append(part)

        assert chunks == [b"Hello, "]
        assert response.is_closed
        assert response.is_stream_consumed

    @pytest.mark.asyncio
    async def test_async_generator_exception_propagates(self):
        async def failing_body():
            yield b"Hello, "
            raise ValueError("boom")

        response = httpx.Response(200, content=failing_body())

        with pytest.raises(ValueError, match="boom"):
            await response.aread()

        assert response.is_closed
        assert response.is_stream_consumed
        with pytest.raises(httpx.StreamConsumed):
            [part async for part in response.aiter_raw()]

    @pytest.mark.asyncio
    async def test_async_generator_exception_partway_through_iter(self):
        async def failing_body():
            yield b"Hello, "
            raise ValueError("boom")

        response = httpx.Response(200, content=failing_body())

        chunks = []
        with pytest.raises(ValueError, match="boom"):
            async for part in response.aiter_raw():
                chunks.append(part)

        assert chunks == [b"Hello, "]
        assert response.is_closed
        assert response.is_stream_consumed

    def test_invalid_content_type_raises_type_error(self):
        with pytest.raises(TypeError):
            httpx.Response(200, content=123)  # type: ignore
