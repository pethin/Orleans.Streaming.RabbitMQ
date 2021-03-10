using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.RabbitMQ.Config;
using Orleans.Streams;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    internal class RabbitMQAdapter : IQueueAdapter, IAsyncDisposable
    {
        protected readonly string ServiceId;
        protected readonly RabbitMQOptions QueueOptions;

        protected IConnection? Connection;

        protected readonly ConcurrentDictionary<QueueId, RabbitMQAdapterSender> Queues = new ConcurrentDictionary<QueueId, RabbitMQAdapterSender>();

        private readonly SerializationManager _serializationManager;
        private readonly IRabbitMQStreamQueueMapper _streamQueueMapper;
        private readonly ILoggerFactory _loggerFactory;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public RabbitMQAdapter(
            SerializationManager serializationManager,
            IRabbitMQStreamQueueMapper streamQueueMapper,
            ILoggerFactory loggerFactory,
            RabbitMQOptions queueOptions,
            string serviceId,
            string providerName)
        {
            _serializationManager = serializationManager;
            QueueOptions = queueOptions;
            ServiceId = serviceId;
            Name = providerName;
            _streamQueueMapper = streamQueueMapper;
            _loggerFactory = loggerFactory;
        }

        private bool IsConnected => Connection != null && Connection.IsOpen;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            if (!IsConnected)
                Connect();

            return new RabbitMQAdapterReceiver(_serializationManager, _loggerFactory, Connection!,
                QueueOptions, _streamQueueMapper.QueueIdToQueueName(queueId));
        }

        public async Task QueueMessageBatchAsync<T>(
            Guid streamGuid,
            string streamNamespace,
            IEnumerable<T> events,
            StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (!IsConnected)
                Connect();

            if (token != null)
                throw new ArgumentException(
                    "RabbitMQ stream provider does not support non-null StreamSequenceToken.",
                    nameof(token));
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            if (!Queues.TryGetValue(queueId, out var queue))
            {
                var tmpQueue = new RabbitMQAdapterSender(_loggerFactory, _serializationManager, Connection!,
                    _streamQueueMapper.QueueIdToQueueName(queueId), QueueOptions);
                await tmpQueue.InitializeAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }

            await queue.SendAsync(streamGuid, streamNamespace, events, requestContext);
        }

        protected void Connect()
        {
            var factory = new ConnectionFactory
            {
                HostName = QueueOptions.HostName,
                VirtualHost = QueueOptions.VirtualHost,
                UserName = QueueOptions.UserName,
                Password = QueueOptions.Password,
                AutomaticRecoveryEnabled = true,
                DispatchConsumersAsync = true,
                UseBackgroundThreadsForIO = true
            };
            Connection = factory.CreateConnection($"{ServiceId}-{Name}");
        }

        public ValueTask DisposeAsync()
        {
            Connection?.Close();
            Connection?.Dispose();
            Connection = null;
            return default;
        }
    }
}