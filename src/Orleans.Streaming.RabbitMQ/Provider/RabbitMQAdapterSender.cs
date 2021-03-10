using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.RabbitMQ.Config;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    public class RabbitMQAdapterSender
    {
        private readonly SerializationManager _serializationManager;
        private readonly IConnection _connection;
        private readonly string _queueName;
        private readonly RabbitMQOptions _queueOptions;
        private IModel? _model;
        private readonly object _lock = new object();

        public RabbitMQAdapterSender(ILoggerFactory loggerFactory, SerializationManager serializationManager,
            IConnection connection, string queueName, RabbitMQOptions queueOptions)
        {
            _serializationManager = serializationManager;
            _connection = connection;
            _queueName = queueName;
            _queueOptions = queueOptions;
        }

        private bool IsConnected => _model != null && _model.IsOpen;

        public Task InitializeAsync()
        {
            Connect();
            return Task.CompletedTask;
        }

        private void Connect()
        {
            lock (_lock)
            {
                if (IsConnected) return;
            
                _model = _connection.CreateModelFromOptions(_queueName, _queueOptions);
                _model.ConfirmSelect();
            }
        }

        public async Task SendAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events,
            Dictionary<string, object> requestContext)
        {
            var eventList = events.Cast<object>().ToList();
            if (eventList.Count == 0)
                return;

            var batch = new RabbitMQBatchContainer(
                streamGuid,
                streamNamespace,
                eventList,
                _queueOptions.ImportRequestContext ? requestContext : null
            );

            if (!IsConnected)
                Connect();

            await Task.Run(() =>
            {
                lock (_lock)
                {
                    _model.BasicPublish("amq.direct", _queueName, basicProperties: null,
                        _serializationManager.SerializeToByteArray(batch));
                    _model?.WaitForConfirmsOrDie();
                }
            });
        }
    }
}