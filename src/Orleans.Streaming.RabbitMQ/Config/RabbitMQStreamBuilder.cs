using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streaming.RabbitMQ.Provider;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Config
{
    public interface IRabbitMQStreamConfigurator : INamedServiceConfigurator { }
    
    public interface ISiloRabbitMQStreamConfigurator : IRabbitMQStreamConfigurator, ISiloPersistentStreamConfigurator { }
    
    public class SiloRabbitMQStreamConfigurator : SiloPersistentStreamConfigurator, ISiloRabbitMQStreamConfigurator
    {
        public SiloRabbitMQStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate,
            Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureServicesDelegate, RabbitMQAdapterFactory.Create)
        {
            configureAppPartsDelegate(RabbitMQStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(RabbitMQOptionsValidator.Create);
            this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);
        }
    }
    
    public interface IClusterClientRabbitMQStreamConfigurator : IRabbitMQStreamConfigurator, IClusterClientPersistentStreamConfigurator { }

    public class ClusterClientRabbitMQStreamConfigurator : ClusterClientPersistentStreamConfigurator, IClusterClientRabbitMQStreamConfigurator
    {
        public ClusterClientRabbitMQStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RabbitMQAdapterFactory.Create)
        {
            builder.ConfigureApplicationParts(RabbitMQStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(RabbitMQOptionsValidator.Create);
        }
    }

    
    public static class RabbitMQStreamConfiguratorExtensions
    {
        public static void ConfigureRabbitMQ(this IRabbitMQStreamConfigurator configurator, Action<OptionsBuilder<RabbitMQOptions>> configureOptions)
        {
            configurator.Configure(configureOptions);
        }
    }
    
    public static class RabbitMQStreamConfiguratorCommon
    {
        public static void AddParts(IApplicationPartManager parts)
        {
            parts.AddFrameworkPart(typeof(RabbitMQAdapterFactory).Assembly);
        }
    }
}