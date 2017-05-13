// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Internal.System.IO.Pipelines;
using Microsoft.Extensions.Internal;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http
{
    public abstract class MessageBody
    {
        private static readonly MessageBody _zeroContentLengthClose = new ForZeroContentLength(keepAlive: false);
        private static readonly MessageBody _zeroContentLengthKeepAlive = new ForZeroContentLength(keepAlive: true);

        private readonly Frame _context;
        private bool _send100Continue = true;

        protected MessageBody(Frame context)
        {
            _context = context;
        }

        public static MessageBody ZeroContentLengthClose => _zeroContentLengthClose;

        public bool RequestKeepAlive { get; protected set; }

        public bool RequestUpgrade { get; protected set; }

        public virtual bool IsEmpty => false;

        public Task<ReadableBuffer> ReadAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var task = OnReadAsync(cancellationToken);

            if (!task.IsCompleted)
            {
                TryProduceContinue();

                // Incomplete Task await result
                return ReadAsyncAwaited(task);
            }
            else
            {
                return task.AsTask();
            }
        }

        private async Task<ReadableBuffer> ReadAsyncAwaited(ValueTask<ReadableBuffer> task)
        {
            return await task;
        }

        public void TryProduceContinue()
        {
            if (_send100Continue)
            {
                _context.FrameControl.ProduceContinue();
                _send100Continue = false;
            }
        }

        protected abstract ValueTask<ReadableBuffer> OnReadAsync(CancellationToken cancellationToken);

        public static MessageBody For(
            HttpVersion httpVersion,
            FrameRequestHeaders headers,
            Frame context)
        {
            // see also http://tools.ietf.org/html/rfc2616#section-4.4
            var keepAlive = httpVersion != HttpVersion.Http10;

            var connection = headers.HeaderConnection;
            var upgrade = false;
            if (connection.Count > 0)
            {
                var connectionOptions = FrameHeaders.ParseConnection(connection);

                upgrade = (connectionOptions & ConnectionOptions.Upgrade) == ConnectionOptions.Upgrade;
                keepAlive = (connectionOptions & ConnectionOptions.KeepAlive) == ConnectionOptions.KeepAlive;
            }

            var transferEncoding = headers.HeaderTransferEncoding;
            if (transferEncoding.Count > 0)
            {
                var transferCoding = FrameHeaders.GetFinalTransferCoding(headers.HeaderTransferEncoding);

                // https://tools.ietf.org/html/rfc7230#section-3.3.3
                // If a Transfer-Encoding header field
                // is present in a request and the chunked transfer coding is not
                // the final encoding, the message body length cannot be determined
                // reliably; the server MUST respond with the 400 (Bad Request)
                // status code and then close the connection.
                if (transferCoding != TransferCoding.Chunked)
                {
                    context.RejectRequest(RequestRejectionReason.FinalTransferCodingNotChunked, transferEncoding.ToString());
                }

                if (upgrade)
                {
                    context.RejectRequest(RequestRejectionReason.UpgradeRequestCannotHavePayload);
                }

                return new ForChunkedEncoding(keepAlive, headers, context);
            }

            if (headers.ContentLength.HasValue)
            {
                var contentLength = headers.ContentLength.Value;

                if (contentLength == 0)
                {
                    return keepAlive ? _zeroContentLengthKeepAlive : _zeroContentLengthClose;
                }
                else if (upgrade)
                {
                    context.RejectRequest(RequestRejectionReason.UpgradeRequestCannotHavePayload);
                }

                return new ForContentLength(keepAlive, contentLength, context);
            }

            // Avoid slowing down most common case
            if (!object.ReferenceEquals(context.Method, HttpMethods.Get))
            {
                // If we got here, request contains no Content-Length or Transfer-Encoding header.
                // Reject with 411 Length Required.
                if (HttpMethods.IsPost(context.Method) || HttpMethods.IsPut(context.Method))
                {
                    var requestRejectionReason = httpVersion == HttpVersion.Http11 ? RequestRejectionReason.LengthRequired : RequestRejectionReason.LengthRequiredHttp10;
                    context.RejectRequest(requestRejectionReason, context.Method);
                }
            }

            if (upgrade)
            {
                return new ForUpgrade(context);
            }

            return keepAlive ? _zeroContentLengthKeepAlive : _zeroContentLengthClose;
        }

        private class ForUpgrade : MessageBody
        {
            public ForUpgrade(Frame context)
                : base(context)
            {
                RequestUpgrade = true;
            }

            protected override ValueTask<ReadableBuffer> OnReadAsync(CancellationToken cancellationToken)
            {
                return _context.Input.ReadReadableBufferAsync();
            }
        }

        private class ForZeroContentLength : MessageBody
        {
            public ForZeroContentLength(bool keepAlive)
                : base(null)
            {
                RequestKeepAlive = keepAlive;
            }

            public override bool IsEmpty => true;

            protected override ValueTask<ReadableBuffer> OnReadAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<ReadableBuffer>();
            }
        }

        private class ForContentLength : MessageBody
        {
            private readonly long _contentLength;
            private long _inputLength;

            public ForContentLength(bool keepAlive, long contentLength, Frame context)
                : base(context)
            {
                RequestKeepAlive = keepAlive;
                _contentLength = contentLength;
                _inputLength = _contentLength;
            }

            protected override ValueTask<ReadableBuffer> OnReadAsync(CancellationToken cancellationToken)
            {
                if (_inputLength == 0)
                {
                    return new ValueTask<ReadableBuffer>();
                }

                var awaitable = _context.Input.ReadReadableBufferAsync();
                if (awaitable.IsCompleted)
                {
                    // .GetAwaiter().GetResult() done by ValueTask if needed
                    var buffer = awaitable.Result;
                    var actual = (int)Math.Min(buffer.Length, _inputLength);

                    if (actual == 0)
                    {
                        _context.RejectRequest(RequestRejectionReason.UnexpectedEndOfRequestContent);
                    }

                    _inputLength -= actual;

                    return new ValueTask<ReadableBuffer>(buffer.Length == actual ? buffer : buffer.Slice(0, actual));
                }
                else
                {
                    return new ValueTask<ReadableBuffer>(OnReadAsyncAwaited(awaitable));
                }
            }

            private async Task<ReadableBuffer> OnReadAsyncAwaited(ValueTask<ReadableBuffer> awaitable)
            {
                var buffer = await awaitable;

                var actual = (int)Math.Min(buffer.Length, _inputLength);

                if (actual == 0)
                {
                    _context.RejectRequest(RequestRejectionReason.UnexpectedEndOfRequestContent);
                }

                _inputLength -= actual;

                return buffer.Length == actual ? buffer : buffer.Slice(0, actual);
            }
        }

        /// <summary>
        ///   http://tools.ietf.org/html/rfc2616#section-3.6.1
        /// </summary>
        private class ForChunkedEncoding : MessageBody
        {
            // byte consts don't have a data type annotation so we pre-cast it
            private const byte ByteCR = (byte)'\r';

            private readonly IPipeReader _input;
            private readonly FrameRequestHeaders _requestHeaders;
            private int _inputLength;

            private Mode _mode = Mode.Prefix;

            public ForChunkedEncoding(bool keepAlive, FrameRequestHeaders headers, Frame context)
                : base(context)
            {
                RequestKeepAlive = keepAlive;
                _input = _context.Input;
                _requestHeaders = headers;
            }

            protected override ValueTask<ReadableBuffer> OnReadAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<ReadableBuffer>(PeekStateMachineAsync());
            }

            private async Task<ReadableBuffer> PeekStateMachineAsync()
            {
                while (_mode < Mode.Trailer)
                {
                    while (_mode == Mode.Prefix)
                    {
                        var result = await _input.ReadAsync();
                        var buffer = result.Buffer;
                        var consumed = default(ReadCursor);
                        var examined = default(ReadCursor);

                        try
                        {
                            ParseChunkedPrefix(buffer, out consumed, out examined);
                        }
                        finally
                        {
                            _input.Advance(consumed, examined);
                        }

                        if (_mode != Mode.Prefix)
                        {
                            break;
                        }
                        else if (result.IsCompleted)
                        {
                            _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                        }

                    }

                    while (_mode == Mode.Extension)
                    {
                        var result = await _input.ReadAsync();
                        var buffer = result.Buffer;
                        var consumed = default(ReadCursor);
                        var examined = default(ReadCursor);

                        try
                        {
                            ParseExtension(buffer, out consumed, out examined);
                        }
                        finally
                        {
                            _input.Advance(consumed, examined);
                        }

                        if (_mode != Mode.Extension)
                        {
                            break;
                        }
                        else if (result.IsCompleted)
                        {
                            _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                        }

                    }

                    while (_mode == Mode.Data)
                    {
                        var result = await _input.ReadAsync();
                        var buffer = result.Buffer;
                        var consumed = default(ReadCursor);
                        var examined = default(ReadCursor);

                        try
                        {
                            var data = PeekChunkedData(buffer, out consumed, out examined);

                            if (data.Length != 0)
                            {
                                return data;
                            }
                            else if (_mode != Mode.Data)
                            {
                                break;
                            }
                            else if (result.IsCompleted)
                            {
                                _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                            }
                        }
                        finally
                        {
                            _input.Advance(consumed, examined);
                        }
                    }

                    while (_mode == Mode.Suffix)
                    {
                        var result = await _input.ReadAsync();
                        var buffer = result.Buffer;
                        var consumed = default(ReadCursor);
                        var examined = default(ReadCursor);

                        try
                        {
                            ParseChunkedSuffix(buffer, out consumed, out examined);
                        }
                        finally
                        {
                            _input.Advance(consumed, examined);
                        }

                        if (_mode != Mode.Suffix)
                        {
                            break;
                        }
                        else if (result.IsCompleted)
                        {
                            _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                        }
                    }
                }

                // Chunks finished, parse trailers
                while (_mode == Mode.Trailer)
                {
                    var result = await _input.ReadAsync();
                    var buffer = result.Buffer;
                    var consumed = default(ReadCursor);
                    var examined = default(ReadCursor);

                    try
                    {
                        ParseChunkedTrailer(buffer, out consumed, out examined);
                    }
                    finally
                    {
                        _input.Advance(consumed, examined);
                    }

                    if (_mode != Mode.Trailer)
                    {
                        break;
                    }
                    else if (result.IsCompleted)
                    {
                        _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                    }

                }

                if (_mode == Mode.TrailerHeaders)
                {
                    while (true)
                    {
                        var result = await _input.ReadAsync();
                        var buffer = result.Buffer;

                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            _context.RejectRequest(RequestRejectionReason.ChunkedRequestIncomplete);
                        }

                        var consumed = default(ReadCursor);
                        var examined = default(ReadCursor);

                        try
                        {
                            if (_context.TakeMessageHeaders(buffer, out consumed, out examined))
                            {
                                break;
                            }
                        }
                        finally
                        {
                            _input.Advance(consumed, examined);
                        }
                    }
                    _mode = Mode.Complete;
                }

                return default(ReadableBuffer);
            }

            private void ParseChunkedPrefix(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;
                var reader = new ReadableBufferReader(buffer);
                var ch1 = reader.Take();
                var ch2 = reader.Take();

                if (ch1 == -1 || ch2 == -1)
                {
                    examined = reader.Cursor;
                    return;
                }

                var chunkSize = CalculateChunkSize(ch1, 0);
                ch1 = ch2;

                do
                {
                    if (ch1 == ';')
                    {
                        consumed = reader.Cursor;
                        examined = reader.Cursor;

                        _inputLength = chunkSize;
                        _mode = Mode.Extension;
                        return;
                    }

                    ch2 = reader.Take();
                    if (ch2 == -1)
                    {
                        examined = reader.Cursor;
                        return;
                    }

                    if (ch1 == '\r' && ch2 == '\n')
                    {
                        consumed = reader.Cursor;
                        examined = reader.Cursor;

                        _inputLength = chunkSize;

                        if (chunkSize > 0)
                        {
                            _mode = Mode.Data;
                        }
                        else
                        {
                            _mode = Mode.Trailer;
                        }

                        return;
                    }

                    chunkSize = CalculateChunkSize(ch1, chunkSize);
                    ch1 = ch2;
                } while (ch1 != -1);
            }

            private void ParseExtension(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                // Chunk-extensions not currently parsed
                // Just drain the data
                consumed = buffer.Start;
                examined = buffer.Start;
                do
                {
                    ReadCursor extensionCursor;
                    if (ReadCursorOperations.Seek(buffer.Start, buffer.End, out extensionCursor, ByteCR) == -1)
                    {
                        // End marker not found yet
                        examined = buffer.End;
                        return;
                    };

                    var sufixBuffer = buffer.Slice(extensionCursor);
                    if (sufixBuffer.Length < 2)
                    {
                        examined = buffer.End;
                        return;
                    }

                    sufixBuffer = sufixBuffer.Slice(0, 2);
                    var sufixSpan = sufixBuffer.ToSpan();


                    if (sufixSpan[1] == '\n')
                    {
                        consumed = sufixBuffer.End;
                        examined = sufixBuffer.End;
                        if (_inputLength > 0)
                        {
                            _mode = Mode.Data;
                        }
                        else
                        {
                            _mode = Mode.Trailer;
                        }
                    }
                } while (_mode == Mode.Extension);
            }

            private ReadableBuffer PeekChunkedData(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;

                if (_inputLength == 0)
                {
                    _mode = Mode.Suffix;
                    return default(ReadableBuffer);
                }

                if (buffer.Length > _inputLength)
                {
                    buffer = buffer.Slice(0, _inputLength);
                }

                consumed = examined = buffer.End;
                _inputLength -= buffer.Length;

                return buffer;
            }

            private void ParseChunkedSuffix(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;

                if (buffer.Length < 2)
                {
                    examined = buffer.End;
                    return;
                }

                var suffixBuffer = buffer.Slice(0, 2);
                var suffixSpan = suffixBuffer.ToSpan();
                if (suffixSpan[0] == '\r' && suffixSpan[1] == '\n')
                {
                    consumed = suffixBuffer.End;
                    examined = suffixBuffer.End;
                    _mode = Mode.Prefix;
                }
                else
                {
                    _context.RejectRequest(RequestRejectionReason.BadChunkSuffix);
                }
            }

            private void ParseChunkedTrailer(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;

                if (buffer.Length < 2)
                {
                    examined = buffer.End;
                    return;
                }

                var trailerBuffer = buffer.Slice(0, 2);
                var trailerSpan = trailerBuffer.ToSpan();

                if (trailerSpan[0] == '\r' && trailerSpan[1] == '\n')
                {
                    consumed = trailerBuffer.End;
                    examined = trailerBuffer.End;
                    _mode = Mode.Complete;
                }
                else
                {
                    _mode = Mode.TrailerHeaders;
                }
            }

            private int CalculateChunkSize(int extraHexDigit, int currentParsedSize)
            {
                checked
                {
                    if (extraHexDigit >= '0' && extraHexDigit <= '9')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - '0');
                    }
                    else if (extraHexDigit >= 'A' && extraHexDigit <= 'F')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - ('A' - 10));
                    }
                    else if (extraHexDigit >= 'a' && extraHexDigit <= 'f')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - ('a' - 10));
                    }
                }

                _context.RejectRequest(RequestRejectionReason.BadChunkSizeData);
                return -1; // can't happen, but compiler complains
            }

            private enum Mode
            {
                Prefix,
                Extension,
                Data,
                Suffix,
                Trailer,
                TrailerHeaders,
                Complete
            };
        }
    }
}
