using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.RabbitMQ.Config;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Provider
{
    public class RabbitMQAdapterFactory : IQueueAdapterFactory
    {
        private readonly string _providerName;
        private readonly RabbitMQOptions _options;
        private readonly ClusterOptions _clusterOptions;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IRabbitMQStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;

        protected SerializationManager SerializationManager { get; }
        
        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { private get; set; }
        
        public RabbitMQAdapterFactory(
            ILoggerFactory loggerFactory,
            string name,
            RabbitMQOptions options,
            SimpleQueueCacheOptions cacheOptions,
            IOptions<ClusterOptions> clusterOptions,
            SerializationManager serializationManager)
        {
            _providerName = name;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _clusterOptions = clusterOptions.Value;
            SerializationManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _streamQueueMapper = new RabbitMQStreamQueueMapper(options, $"{options.QueuePrefix}-{name}");
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, _loggerFactory);
        }

        public virtual void Init()
        {
            StreamFailureHandlerFactory ??= _ => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
        }
        
        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RabbitMQAdapter(
                SerializationManager,
                _streamQueueMapper,
                _loggerFactory,
                _options,
                _clusterOptions.ServiceId,
                _providerName);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        public Task<IStreamFailureHandler>? GetDeliveryFailureHandler(QueueId queueId)
        {
            return StreamFailureHandlerFactory?.Invoke(queueId);
        }
        
        public static RabbitMQAdapterFactory Create(IServiceProvider services, string name)
        {
            var rabbitMqOptions = services.GetOptionsByName<RabbitMQOptions>(name);
            var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
            var clusterOptions = services.GetProviderClusterOptions(name);
            var factory = ActivatorUtilities.CreateInstance<RabbitMQAdapterFactory>(services, name, rabbitMqOptions, cacheOptions, clusterOptions);
            factory.Init();
            return factory;
        }
    }
}