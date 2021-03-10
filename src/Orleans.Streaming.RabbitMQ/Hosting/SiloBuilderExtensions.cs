using System;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Streaming.RabbitMQ.Config;

namespace Orleans.Streaming.RabbitMQ.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use RabbitMQ persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMQStreams(this ISiloHostBuilder builder, string name,
            Action<SiloRabbitMQStreamConfigurator> configure)
        {
            var configurator = new SiloRabbitMQStreamConfigurator(name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure silo to use RabbitMQ persistent streams with default settings
        /// </summary>
        public static ISiloHostBuilder AddRabbitMQStreams(this ISiloHostBuilder builder, string name,
            Action<OptionsBuilder<RabbitMQOptions>> configureOptions)
        {
            builder.AddRabbitMQStreams(name, b => b.ConfigureRabbitMQ(configureOptions));
            return builder;
        }

        /// <summary>
        /// Configure silo to use RabbitMQ persistent streams.
        /// </summary>
        public static ISiloBuilder AddRabbitMQStreams(this ISiloBuilder builder, string name,
            Action<SiloRabbitMQStreamConfigurator> configure)
        {
            var configurator = new SiloRabbitMQStreamConfigurator(name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure silo to use RabbitMQ persistent streams with default settings
        /// </summary>
        public static ISiloBuilder AddRabbitMQStreams(this ISiloBuilder builder, string name,
            Action<OptionsBuilder<RabbitMQOptions>> configureOptions)
        {
            builder.AddRabbitMQStreams(name, b => b.ConfigureRabbitMQ(configureOptions));
            return builder;
        }
    }
}