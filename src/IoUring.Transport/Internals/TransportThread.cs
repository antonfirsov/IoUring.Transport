using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using IoUring.Concurrent;
using IoUring.Transport.Internals.Inbound;
using IoUring.Transport.Internals.Outbound;
using Microsoft.AspNetCore.Connections;
using Tmds.Linux;
using static Tmds.Linux.LibC;

namespace IoUring.Transport.Internals
{
    internal sealed unsafe class TransportThread : IAsyncDisposable
    {
        private const int RingSize = 4096;
        private const int ListenBacklog = 128;
        private const ulong ReadMask =         (ulong) OperationType.Read << 32;
        private const ulong WriteMask =        (ulong) OperationType.Write << 32;
        private const ulong EventFdPollMask =  (ulong) OperationType.EventFdPoll << 32;
        private const ulong ConnectMask =      (ulong) OperationType.Connect << 32;
        private const ulong AcceptMask =       (ulong) OperationType.Accept << 32;

        private static int _threadId;

        private readonly ConcurrentRing _ring = new ConcurrentRing(RingSize);
        private readonly ConcurrentDictionary<int, AcceptSocketContext> _acceptSockets = new ConcurrentDictionary<int, AcceptSocketContext>();
        private readonly ConcurrentDictionary<int, IoUringConnectionContext> _connections = new ConcurrentDictionary<int, IoUringConnectionContext>();
        private readonly TaskCompletionSource<object> _threadCompletion = new TaskCompletionSource<object>();
        private readonly TransportThreadContext _threadContext;
        private readonly int _maxBufferSize;
        private readonly int _eventfd;
        private readonly GCHandle _eventfdBytes;
        private readonly GCHandle _eventfdIoVecHandle;
        private readonly iovec* _eventfdIoVec;

        private volatile bool _disposed;

        public TransportThread(IoUringOptions options)
        {
            int res = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (res == -1) throw new ErrnoException(errno);
            _eventfd = res;

            // Pin buffer for eventfd reads via io_uring
            byte[] bytes = new byte[8];
            _eventfdBytes = GCHandle.Alloc(bytes, GCHandleType.Pinned);

            // Pin iovec used for eventfd reads via io_uring
            var eventfdIoVec = new iovec
            {
                iov_base = (void*) _eventfdBytes.AddrOfPinnedObject(),
                iov_len = bytes.Length
            };
            _eventfdIoVecHandle = GCHandle.Alloc(eventfdIoVec, GCHandleType.Pinned);
            _eventfdIoVec = (iovec*) _eventfdIoVecHandle.AddrOfPinnedObject();

            var memoryPool = new SlabMemoryPool();
            _threadContext = new TransportThreadContext(options, memoryPool, _eventfd, s => Read(s), s => Write(s));
            _maxBufferSize = memoryPool.MaxBufferSize;
        }

        public ValueTask<ConnectionContext> Connect(IPEndPoint endpoint)
        {
            var domain = endpoint.AddressFamily == AddressFamily.InterNetwork ? AF_INET : AF_INET6;
            LinuxSocket s = socket(domain, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
            if (_threadContext.Options.TcpNoDelay)
            {
                s.SetOption(SOL_TCP, TCP_NODELAY, 1);
            }

            var context = new OutboundConnectionContext(s, endpoint, _threadContext);
            var tcs = new TaskCompletionSource<ConnectionContext>(TaskCreationOptions.RunContinuationsAsynchronously); // Ensure the transport thread doesn't run continuations
            context.ConnectCompletion = tcs;

            endpoint.ToSockAddr(context.Addr, out var addrLength);
            context.AddrLen = addrLength;

            _connections[s] = context;
            Connect(context);

            _threadContext.Notify();

            return new ValueTask<ConnectionContext>(tcs.Task);
        }

        public void Bind(IPEndPoint endpoint, ChannelWriter<ConnectionContext> acceptQueue)
        {
            var domain = endpoint.AddressFamily == AddressFamily.InterNetwork ? AF_INET : AF_INET6;
            LinuxSocket s = socket(domain, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
            s.SetOption(SOL_SOCKET, SO_REUSEADDR, 1);
            s.SetOption(SOL_SOCKET, SO_REUSEPORT, 1);
            s.Bind(endpoint);
            s.Listen(ListenBacklog);

            var context = new AcceptSocketContext(s, endpoint, acceptQueue);

            _acceptSockets[s] = context;
            Accept(context);

            _threadContext.Notify();
        }

        public void Run() => new Thread(obj => ((TransportThread)obj).Loop())
        {
            IsBackground = true,
            Name = $"IoUring Transport Thread - {Interlocked.Increment(ref _threadId)}"
        }.Start(this);

        private void Loop()
        {
            ReadEventFd();

            while (!_disposed)
            {
                Submit();
                Complete();
            }

            _ring.Dispose();
            _threadCompletion.TrySetResult(null);
        }

        private void ReadEventFd()
        {
            Debug.WriteLine("Adding read on eventfd");

            Span<Submission> submissions = stackalloc Submission[2];
            _ring.AcquireSubmissions(submissions);

            submissions[0].PreparePollAdd(_eventfd, (ushort)POLLIN, options: SubmissionOption.Link);
            _ring.Release(submissions[0]);

            submissions[1].PrepareReadV(_eventfd, _eventfdIoVec, 1, userData: EventFdPollMask);
            _ring.Release(submissions[1]);
        }

        private void Accept(AcceptSocketContext context)
        {
            var socket = context.Socket;
            Debug.WriteLine($"Adding accept on {(int)socket}");

            _ring.AcquireSubmission(out var submission);
            submission.PrepareAccept(socket, (sockaddr*) context.Addr, context.AddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC, Mask(socket, AcceptMask));
            _ring.Release(submission);
        }

        private void Connect(OutboundConnectionContext context)
        {
            var socket = context.Socket;
            Debug.WriteLine($"Adding connect on {(int)socket}");

            _ring.AcquireSubmission(out var submission);
            submission.PrepareConnect(socket, (sockaddr*) context.Addr, context.AddrLen, Mask(socket, ConnectMask));
            _ring.Release(submission);
        }

        private void Read(int socket) => Read(_connections[socket]);

        private void Read(IoUringConnectionContext context)
        {
            var writer = context.Input;
            var readHandles = context.ReadHandles;
            var readVecs = context.ReadVecs;

            var memory = writer.GetMemory(_maxBufferSize);
            var handle = memory.Pin();

            readVecs[0].iov_base = handle.Pointer;
            readVecs[0].iov_len = memory.Length;

            readHandles[0] = handle;

            var socket = context.Socket;
            Debug.WriteLine($"Adding read on {(int)socket}");

            Span<Submission> submissions = stackalloc Submission[2];
            _ring.AcquireSubmissions(submissions);

            submissions[0].PreparePollAdd(socket, (ushort) POLLIN, options: SubmissionOption.Link);
            _ring.Release(submissions[0]);

            submissions[1].PrepareReadV(socket, readVecs, 1, 0, 0, Mask(socket, ReadMask));
            _ring.Release(submissions[1]);
        }

        private void Write(int socket) => Write(_connections[socket]);

        private void Write(IoUringConnectionContext context)
        {
            var result = context.ReadResult.Result;
            var buffer = result.Buffer;
            var socket = context.Socket;
            if ((buffer.IsEmpty && result.IsCompleted) || result.IsCanceled)
            {
                context.DisposeAsync();
                _connections.TryRemove(socket, out _);
                return;
            }

            var writeHandles = context.WriteHandles;
            var writeVecs = context.WriteVecs;
            int ctr = 0;
            foreach (var memory in buffer)
            {
                var handle = memory.Pin();

                writeVecs[ctr].iov_base = handle.Pointer;
                writeVecs[ctr].iov_len = memory.Length;

                writeHandles[ctr] = handle;

                ctr++;
                if (ctr == IoUringConnectionContext.WriteIOVecCount) break;
            }

            context.LastWrite = buffer;

            Debug.WriteLine($"Adding write on {(int)socket}");

            Span<Submission> submissions = stackalloc Submission[2];
            _ring.AcquireSubmissions(submissions);

            submissions[0].PreparePollAdd(socket, (ushort) POLLOUT, options: SubmissionOption.Link);
            _ring.Release(submissions[0]);

            submissions[1].PrepareWriteV(socket, writeVecs ,ctr, 0 ,0, Mask(socket, WriteMask));
            _ring.Release(submissions[1]);
        }

        private void Submit()
        {
            uint minComplete;
            if (_ring.SubmissionEntriesUsed == 0)
            {
                minComplete = 1;
                _threadContext.SetBlockingMode(true);
            }
            else
            {
                minComplete = 0;
            }

            _ring.SubmitAndWait(minComplete, out _);
            _threadContext.SetBlockingMode(false);
        }

        private void Complete()
        {
            int ctr = 0;
            while (_ring.TryRead(out Completion c))
            {
                var socket = (int) c.userData;
                var operationType = (OperationType) (c.userData >> 32);

                switch (operationType)
                {
                    case OperationType.EventFdPoll:
                        CompleteEventFdPoll();
                        break;
                    case OperationType.Accept:
                        CompleteAccept(_acceptSockets[socket], c.result);
                        break;
                    case OperationType.Read:
                        CompleteRead(_connections[socket], c.result);
                        break;
                    case OperationType.Write:
                        CompleteWrite(_connections[socket], c.result);
                        break;
                    case OperationType.Connect:
                        CompleteConnect((OutboundConnectionContext) _connections[socket], c.result);
                        break;
                }

                ctr++;
            }
            
            Debug.WriteLine($"Completed {ctr}");
        }

        private void CompleteEventFdPoll()
        {
            Debug.WriteLine("Completed eventfd poll");
            ReadEventFd();
        }

        private void CompleteAccept(AcceptSocketContext acceptContext, int result)
        {
            if (result < 0)
            {
                var err = -result;
                if (err == EAGAIN || err == EINTR || err == EMFILE)
                {
                    Debug.WriteLine($"accept completed with EAGAIN on {(int)acceptContext.Socket}");
                    Accept(acceptContext);
                    return;
                }

                throw new ErrnoException(err);
            }

            LinuxSocket socket = result;
            Debug.WriteLine($"Accepted {(int)socket} from {(int)acceptContext.Socket}");
            if (_threadContext.Options.TcpNoDelay)
            {
                socket.SetOption(SOL_TCP, TCP_NODELAY, 1);
            }

            var remoteEndpoint = IPEndPointFormatter.AddrToIpEndPoint(acceptContext.Addr);
            var context = new InboundConnectionContext(socket, acceptContext.EndPoint, remoteEndpoint, _threadContext);

            _connections[socket] = context;
            acceptContext.AcceptQueue.TryWrite(context);

            Accept(acceptContext);
            Read(context);
            ReadFromApp(context);
        }

        private void CompleteConnect(OutboundConnectionContext context, int result)
        {
            var completion = context.ConnectCompletion;
            if (result < 0)
            {
                if (-result != EAGAIN || -result != EINTR)
                {
                    context.ConnectCompletion = null;
                    completion.TrySetException(new ErrnoException(-result));
                }

                Debug.WriteLine($"connect completed with EAGAIN on {(int)context.Socket}");
                Connect(context);
                return;
            }

            Debug.WriteLine($"Connected to {(int)context.Socket}");

            context.ConnectCompletion = null; // no need to hold on to this

            context.LocalEndPoint = context.Socket.GetLocalAddress();
            completion.TrySetResult(context);

            Read(context);
            ReadFromApp(context);
        }

        private void CompleteRead(IoUringConnectionContext context, int result)
        {
            var readHandles = context.ReadHandles;
            for (int i = 0; i < readHandles.Length; i++)
            {
                readHandles[i].Dispose();
            }

            if (result > 0)
            {
                Debug.WriteLine($"Read {result} from {(int)context.Socket}");
                context.Input.Advance(result);
                FlushRead(context);
            }
            else if (result < 0)
            {
                if (-result == ECONNRESET)
                {
                    context.DisposeAsync();
                }
                else if (-result != EAGAIN && -result != EWOULDBLOCK && -result != EINTR)
                {
                    throw new ErrnoException(-result);
                }

                Debug.WriteLine($"Read completed with EAGAIN from {(int)context.Socket}");
            }
            else
            {
                context.DisposeAsync();
            }
        }

        private void FlushRead(IoUringConnectionContext context)
        {
            var flushResult = context.Input.FlushAsync();
            context.FlushResult = flushResult;
            if (flushResult.IsCompletedSuccessfully)
            {
                Debug.WriteLine($"Flushed to app synchronously {(int)context.Socket}");
                // likely
                context.FlushedToAppSynchronously();
                Read(context);
                return;
            }

            flushResult.GetAwaiter().UnsafeOnCompleted(context.OnFlushedToApp);
        }

        private void CompleteWrite(IoUringConnectionContext context, int result)
        {
            var writeHandles = context.WriteHandles;
            for (int i = 0; i < writeHandles.Length; i++)
            {
                writeHandles[i].Dispose();
            }

            var lastWrite = context.LastWrite;
            if (result >= 0)
            {
                Debug.WriteLine($"Read {result} from {(int)context.Socket}");
                SequencePosition end;
                if (result == 0)
                {
                    end = lastWrite.Start;
                }
                else if (lastWrite.Length == result)
                {
                    end = lastWrite.End;
                }
                else
                {
                    end = lastWrite.GetPosition(result);
                }

                context.Output.AdvanceTo(end);
                ReadFromApp(context);
            }
            else
            {
                if (-result == ECONNRESET || -result == EPIPE)
                {
                    context.DisposeAsync();
                }
                else if (-result == EAGAIN || -result == EWOULDBLOCK || -result == EINTR)
                {
                    Debug.WriteLine($"Read completed with EAGAIN from {(int)context.Socket}");
                    context.Output.AdvanceTo(lastWrite.Start);
                    ReadFromApp(context);
                }
                else
                {
                    throw new ErrnoException(-result);
                }
            }
        }

        private void ReadFromApp(IoUringConnectionContext context)
        {
            var readResult = context.Output.ReadAsync();
            context.ReadResult = readResult;
            if (readResult.IsCompletedSuccessfully)
            {
                Debug.WriteLine($"Read from app synchronously {(int)context.Socket}");
                // unlikely
                context.ReadFromAppSynchronously();
                Write(context);
                return;
            }

            readResult.GetAwaiter().UnsafeOnCompleted(context.OnReadFromApp);
        }

        private static ulong Mask(int socket, ulong mask)
        {
            var socketUl = (ulong)socket;
            return socketUl | mask;
        }

        public ValueTask DisposeAsync()
        {
            _disposed = true;
            return new ValueTask(_threadCompletion.Task);
        }
    }
}