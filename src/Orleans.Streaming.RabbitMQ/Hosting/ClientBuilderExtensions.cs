using System;
using Microsoft.Extensions.Options;
using Orleans.Streaming.RabbitMQ.Config;

namespace Orleans.Streaming.RabbitMQ.Hosting
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure cluster client to use RabbitMQ persistent streams.
        /// </summary>
        public static IClientBuilder AddRabbitMQStreams(this IClientBuilder builder,
            string name,
            Action<ClusterClientRabbitMQStreamConfigurator> configure)
        {
            // The constructor wires up DI with RabbitMQStream, so it has to be called regardless configure is null or not
            var configurator = new ClusterClientRabbitMQStreamConfigurator(name, builder);
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure cluster client to use RabbitMQ persistent streams.
        /// </summary>
        public static IClientBuilder AddRabbitMQStreams(this IClientBuilder builder,
            string name, Action<OptionsBuilder<RabbitMQOptions>> configureOptions)
        {
            builder.AddRabbitMQStreams(name, b => b.ConfigureRabbitMQ(configureOptions));
            return builder;
        }
    }
}