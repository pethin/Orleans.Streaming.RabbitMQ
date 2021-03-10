using System;
using System.Collections.Generic;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ.Config;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    public interface IRabbitMQStreamQueueMapper : IStreamQueueMapper
    {
        public string QueueIdToQueueName(QueueId queueId);
    }

    public class RabbitMQStreamQueueMapper : HashRingBasedStreamQueueMapper, IRabbitMQStreamQueueMapper
    {
        public RabbitMQStreamQueueMapper(RabbitMQOptions options, string queueNamePrefix) : base(
            GetHashRingStreamQueueMapperOptions(options), queueNamePrefix)
        {
        }

        private static HashRingStreamQueueMapperOptions GetHashRingStreamQueueMapperOptions(RabbitMQOptions options) =>
            new HashRingStreamQueueMapperOptions
            {
                TotalQueueCount = options.NumberOfQueues
            };

        public string QueueIdToQueueName(QueueId queueId)
        {
            return queueId.ToString();
        }
    }
}