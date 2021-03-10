using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    [Serializable]
    public class RabbitMQBatchContainer : IBatchContainer
    {
        [NonSerialized] internal ulong DeliveryTag;

        private readonly Dictionary<string, object>? _requestContext;
        protected List<object> Events { get; set; }

        public RabbitMQBatchContainer(Guid streamGuid, string streamNamespace, List<object> events,
            Dictionary<string, object>? requestContext)
        {
            Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _requestContext = requestContext;
        }

        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken? SequenceToken { get; internal set; }

        public IEnumerable<Tuple<T, StreamSequenceToken?>> GetEvents<T>()
        {
            var sequenceToken = (EventSequenceTokenV2?) SequenceToken;
            return Events
                .OfType<T>()
                .Select((@event, iteration) =>
                    Tuple.Create<T, StreamSequenceToken?>(@event, sequenceToken?.CreateSequenceTokenForEvent(iteration))
                );
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            // If there is something in this batch that the consumer is interested in, we should send it
            // else the consumer is not interested in any of these events, so don't send.
            return Events.Any(item => shouldReceiveFunc(stream, filterData, item));
        }

        public bool ImportRequestContext()
        {
            if (_requestContext == null)
                return false;

            foreach (var (key, value) in _requestContext)
                RequestContext.Set(key, value);

            return true;
        }

        public override string ToString() => $"[{GetType().Name}:Stream={StreamGuid},#Items={Events.Count}]";
    }
}