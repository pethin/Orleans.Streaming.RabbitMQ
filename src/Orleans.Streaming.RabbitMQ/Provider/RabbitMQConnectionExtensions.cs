using System.Collections.Generic;
using Orleans.Streaming.RabbitMQ.Config;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    public static class RabbitMQConnectionExtensions
    {
        public static IModel CreateModelFromOptions(this IConnection connection, string queueName, RabbitMQOptions options)
        {
            var model = connection.CreateModel();

            var queueArgs = new Dictionary<string, object>
            {
                {"x-expires", options.QueueTtlMs},
                {"x-message-ttl", options.MessageTtlMs},
            };
            if (options.MaxQueueLength != null)
            {
                queueArgs.Add("x-max-length", options.MaxQueueLength);
            }

            if (options.MaxQueueSizeBytes != null)
            {
                queueArgs.Add("x-max-length-bytes", options.MaxQueueSizeBytes);
            }

            model.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
            model.QueueBind(queueName, "amq.direct", queueName);

            return model;
        }
    }
}