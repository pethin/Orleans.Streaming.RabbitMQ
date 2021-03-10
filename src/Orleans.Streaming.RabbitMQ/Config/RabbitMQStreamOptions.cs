using System;
using Orleans.Runtime;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ.Config
{
    public class RabbitMQOptions
    {
        public string? HostName { get; set; }
        public string VirtualHost { get; set; } = ConnectionFactory.DefaultVHost;
        public string? UserName { get; set; }
        [Redact] public string? Password { get; set; }
        
        public ushort NumberOfQueues { get; set; } = 8;
        public string QueuePrefix { get; set; } = "orleans-streaming";

        public uint QueueTtlMs { get; set; } = (uint) TimeSpan.FromDays(1).TotalMilliseconds;
        public uint MessageTtlMs { get; set; } = (uint) TimeSpan.FromDays(1).TotalMilliseconds;
        public uint? MaxQueueLength { get; set; }
        public uint? MaxQueueSizeBytes { get; set; }
        public bool ImportRequestContext { get; set; } = false;
    }

    public class RabbitMQOptionsValidator : IConfigurationValidator
    {
        private readonly RabbitMQOptions _options;
        private readonly string _name;

        private RabbitMQOptionsValidator(RabbitMQOptions options, string name)
        {
            _options = options;
            _name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(_options.HostName))
                throw new OrleansConfigurationException(
                    $"{nameof(RabbitMQOptions)} on stream provider {this._name} is invalid. {nameof(RabbitMQOptions.HostName)} is invalid");
            
            if (string.IsNullOrEmpty(_options.UserName))
                throw new OrleansConfigurationException(
                    $"{nameof(RabbitMQOptions)} on stream provider {this._name} is invalid. {nameof(RabbitMQOptions.UserName)} is invalid");
            
            if (string.IsNullOrEmpty(_options.Password))
                throw new OrleansConfigurationException(
                    $"{nameof(RabbitMQOptions)} on stream provider {this._name} is invalid. {nameof(RabbitMQOptions.Password)} is invalid");
            
            if (_options.NumberOfQueues == 0)
                throw new OrleansConfigurationException(
                    $"{nameof(RabbitMQOptions)} on stream provider {this._name} is invalid. {nameof(RabbitMQOptions.NumberOfQueues)} must be greater than 0");
        }

        public static IConfigurationValidator Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<RabbitMQOptions>(name);
            return new RabbitMQOptionsValidator(options, name);
        }
    }
}