using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Tmds.Linux;

namespace IoUring.Transport.Internals
{
    internal abstract unsafe class IoUringConnectionContext : TransportConnection
    {
        public const int ReadIOVecCount = 1;
        public const int WriteIOVecCount = 8;

        // Copied from LibuvTransportOptions.MaxReadBufferSize
        private const int PauseInputWriterThreshold = 1024 * 1024;
        // Copied from LibuvTransportOptions.MaxWriteBufferSize
        private const int PauseOutputWriterThreshold = 64 * 1024;

        private readonly TransportThreadContext _threadContext;
        private readonly Action _onOnFlushedToApp;
        private readonly Action _onReadFromApp;

        private readonly iovec* _iovec;
        private GCHandle _iovecHandle;

        protected IoUringConnectionContext(LinuxSocket socket, EndPoint local, EndPoint remote, TransportThreadContext threadContext)
        {
            Socket = socket;

            LocalEndPoint = local;
            RemoteEndPoint = remote;

            MemoryPool = threadContext.MemoryPool;
            _threadContext = threadContext;

            var appScheduler = threadContext.Options.ApplicationSchedulingMode;
            var inputOptions = new PipeOptions(MemoryPool, appScheduler, PipeScheduler.Inline, PauseInputWriterThreshold, PauseInputWriterThreshold / 2, useSynchronizationContext: false);
            var outputOptions = new PipeOptions(MemoryPool, PipeScheduler.Inline, appScheduler, PauseOutputWriterThreshold, PauseOutputWriterThreshold / 2, useSynchronizationContext: false);

            var pair = DuplexPipe.CreateConnectionPair(inputOptions, outputOptions);

            Transport = pair.Transport;
            Application = pair.Application;

            _onOnFlushedToApp = FlushedToAppAsynchronously;
            _onReadFromApp = ReadFromAppAsynchronously;

            iovec[] vecs = new iovec[ReadIOVecCount + WriteIOVecCount];
            var vecsHandle = GCHandle.Alloc(vecs, GCHandleType.Pinned);
            _iovec = (iovec*) vecsHandle.AddrOfPinnedObject();
            _iovecHandle = vecsHandle;
        }

        public LinuxSocket Socket { get; }
        public override MemoryPool<byte> MemoryPool { get; }

        public iovec* ReadVecs => _iovec;
        public iovec* WriteVecs => _iovec + ReadIOVecCount;

        public MemoryHandle[] ReadHandles { get; } = new MemoryHandle[ReadIOVecCount];
        public MemoryHandle[] WriteHandles { get; } = new MemoryHandle[WriteIOVecCount];

        public ReadOnlySequence<byte> LastWrite { get; set; }

        public PipeWriter Input => Application.Output;

        public PipeReader Output => Application.Input;

        public ValueTask<FlushResult> FlushResult { get; set; }
        public ValueTask<ReadResult> ReadResult { get; set; }
        public Action OnFlushedToApp => _onOnFlushedToApp;
        public Action OnReadFromApp => _onReadFromApp;

        private void FlushedToApp(bool async)
        {
            var flushResult = FlushResult;
            if (flushResult.IsCanceled || flushResult.IsFaulted)
            {
                DisposeAsync();
                return;
            }

            if (async)
            {
                _threadContext.ScheduleAsyncRead(Socket);
            }
        }

        public void FlushedToAppSynchronously() => FlushedToApp(false);
        private void FlushedToAppAsynchronously() => FlushedToApp(true);

        private void ReadFromApp(bool async)
        {
            var readResult = ReadResult;
            if (readResult.IsCanceled || readResult.IsFaulted)
            {
                DisposeAsync();
                return;
            }

            if (async)
            {
                _threadContext.ScheduleAsyncWrite(Socket);
            }
        }

        private void ReadFromAppAsynchronously() => ReadFromApp(true);
        public void ReadFromAppSynchronously() => ReadFromApp(false);

        public override ValueTask DisposeAsync()
        {
            Socket.Close();
            // TODO: close pipes
            // TODO: close socket fd
            if (_iovecHandle.IsAllocated)
                _iovecHandle.Free();

            return base.DisposeAsync();
        }

        internal class DuplexPipe : IDuplexPipe
        {
            public DuplexPipe(PipeReader reader, PipeWriter writer)
            {
                Input = reader;
                Output = writer;
            }

            public PipeReader Input { get; }

            public PipeWriter Output { get; }

            public static DuplexPipePair CreateConnectionPair(PipeOptions inputOptions, PipeOptions outputOptions)
            {
                var input = new Pipe(inputOptions);
                var output = new Pipe(outputOptions);

                var transportToApplication = new DuplexPipe(output.Reader, input.Writer);
                var applicationToTransport = new DuplexPipe(input.Reader, output.Writer);

                return new DuplexPipePair(applicationToTransport, transportToApplication);
            }

            // This class exists to work around issues with value tuple on .NET Framework
            public readonly struct DuplexPipePair
            {
                public IDuplexPipe Transport { get; }
                public IDuplexPipe Application { get; }

                public DuplexPipePair(IDuplexPipe transport, IDuplexPipe application)
                {
                    Transport = transport;
                    Application = application;
                }
            }
        }
    }
}