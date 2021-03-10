using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.RabbitMQ.Config;
using Orleans.Streams;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    /// <summary>
    /// Receives batches of messages from a single subscription of a topic.
    /// </summary>
    internal class RabbitMQAdapterReceiver : IQueueAdapterReceiver, IAsyncDisposable
    {
        private ILogger _logger;
        private readonly SerializationManager _serializationManager;
        private readonly IConnection _connection;
        private readonly RabbitMQOptions _queueOptions;
        private readonly string _queueName;
        private bool _shutdownRequested;
        private readonly object _lock = new object();
        private IModel? _model;
        private AsyncEventingBasicConsumer? _consumer;
        private readonly ConcurrentQueue<IBatchContainer> _batchQueue = new ConcurrentQueue<IBatchContainer>();

        public RabbitMQAdapterReceiver(SerializationManager serializationManager,
            ILoggerFactory loggerFactory, IConnection connection, RabbitMQOptions queueOptions, string queueName)
        {
            _logger = loggerFactory.CreateLogger<RabbitMQAdapterReceiver>();
            _serializationManager = serializationManager;
            _connection = connection;
            _queueOptions = queueOptions;
            _queueName = queueName;
        }

        private bool IsConnected => _model != null && _model.IsOpen;

        public Task Initialize(TimeSpan timeout)
        {
            Connect();
            return Task.CompletedTask;
        }

        private async Task AddMessagesToQueue(object channel, BasicDeliverEventArgs eventArgs)
        {
            var body = eventArgs.Body.ToArray();
            var batchContainer = _serializationManager.DeserializeFromByteArray<RabbitMQBatchContainer>(body);
            batchContainer.DeliveryTag = eventArgs.DeliveryTag;
            batchContainer.SequenceToken = new EventSequenceTokenV2((long) eventArgs.DeliveryTag);
            
            _batchQueue.Enqueue(batchContainer);
            await Task.Yield();
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batches = new List<IBatchContainer>();

            for (var i = 0; i < maxCount && !_shutdownRequested; i++)
            {
                if (!IsConnected)
                    Connect();

                _batchQueue.TryDequeue(out var batchContainer);
                if (batchContainer == null)
                    break;

                batches.Add(batchContainer);
            }

            return Task.FromResult((IList<IBatchContainer>) batches);
        }
        
        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var acks = new List<Task>(messages.Count);
            acks.AddRange(messages.Cast<RabbitMQBatchContainer>()
                .Select(batch => Task.Run(() => _model?.BasicAck(batch.DeliveryTag, false))));
            return Task.WhenAll(acks);
        }

        public Task Shutdown(TimeSpan timeout)
        {
            try
            {
                _shutdownRequested = true;
                _model?.Close();
                _model?.Dispose();
                _model = null;
            }
            catch (Exception)
            {
                _logger.LogWarning("Receiver already closed, ignoring...");
            }

            return Task.CompletedTask;
        }

        private void Connect()
        {
            lock (_lock)
            {
                if (IsConnected) return;
                _model = _connection.CreateModelFromOptions(_queueName, _queueOptions);
                _model.BasicQos(0, 1024, false);
                _consumer = new AsyncEventingBasicConsumer(_model);
                _consumer.Received += AddMessagesToQueue;
                _model.BasicConsume(_queueName, false, _consumer);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Shutdown(TimeSpan.MaxValue);
        }
    }
}