using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using static Tmds.Linux.LibC;

namespace IoUring.Transport.Internals
{
    internal sealed class TransportThreadContext
    {
        private const int True = 1;
        private const int False = 0;

        private readonly int _eventFd;
        private readonly Action<int> _readAction;
        private readonly Action<int> _writeAction;
        private int _blockingMode;

        public TransportThreadContext(IoUringOptions options, MemoryPool<byte> memoryPool, int eventFd, Action<int> readAction, Action<int> writeAction)
        {
            Options = options;
            MemoryPool = memoryPool;
            _eventFd = eventFd;
            _readAction = readAction;
            _writeAction = writeAction;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBlockingMode(bool blocking) 
            => Volatile.Write(ref _blockingMode, blocking ? True : False);

        public IoUringOptions Options { get; }

        public MemoryPool<byte> MemoryPool { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ShouldUnblock() 
            => Interlocked.CompareExchange(ref _blockingMode, False, True) == True;

        public unsafe void Notify()
        {
            if (!ShouldUnblock())
            {
                // If the transport thread is not (yet) in blocking mode, we have the guarantee, that it will read 
                // from the queues one more time before actually blocking. Therefore, it is safe not to notify now.
                return;
            }

            // The transport thread reported it is (probably still) blocking. We therefore must notify it by writing
            // to the eventfd established for that purpose.

            Debug.WriteLine("Attempting to unblock thread");

            byte* val = stackalloc byte[sizeof(ulong)];
            Unsafe.WriteUnaligned(val, 1UL);
            int rv;
            do
            {
                rv = (int) write(_eventFd, val, sizeof(ulong));
            } while (rv == -1 && errno == EINTR);
        }

        public void ScheduleAsyncRead(int socket)
        {
            Debug.WriteLine($"Wrote to app asynchronously for {socket}");
            _readAction(socket);
            Notify();
        }

        public void ScheduleAsyncWrite(int socket)
        {
            Debug.WriteLine($"Read from app asynchronously for {socket}");
            _writeAction(socket);
            Notify();
        }
    }
}