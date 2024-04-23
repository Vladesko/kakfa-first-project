using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaClient
{
    public class KafkaConsumer //: BackgroundService
    {
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IKafkaHandler _kafkaHandler;
        private readonly string _topicName;
        private bool _isCommited;

        public KafkaConsumer(ILogger<KafkaConsumer> logger, IKafkaHandler kafkaHandler)
        {
            _logger = logger;
            _kafkaHandler = kafkaHandler;
            _topicName = "kafka-first-topic";
        }

        public async Task Consume(CancellationToken cancellationToken)
        {
            //await Task.Yield();

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "consumer-group-kafka-first-topic",
                EnableAutoCommit = false,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
                EnablePartitionEof = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                EnableSslCertificateVerification = false,
                SecurityProtocol = SecurityProtocol.Plaintext
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    _logger.LogInformation(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    _logger.LogInformation(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    _logger.LogInformation($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
            .Build())
            {
                try
                {
                    consumer.Subscribe(_topicName);

                    _logger.LogInformation($"Kafka consumer with Group_Id: {config.GroupId} has been started...");
                    while (cancellationToken.IsCancellationRequested is false)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);
                            _isCommited = false;
                            if (consumeResult.IsPartitionEOF)
                            {
                                _logger.LogInformation($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            if (consumeResult?.Message != null && consumeResult?.Message?.Value != null)
                            {
                                Console.WriteLine($"Got a new message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                                _isCommited = await _kafkaHandler.MessageHandle(consumeResult, cancellationToken);
                            }

                            if (config.EnableAutoCommit == false && _isCommited is true)
                            {
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    _logger.LogError($"Commit error: {e.Error.Reason}");
                                }
                            }
                            if (config.EnableAutoCommit == true && config.EnableAutoOffsetStore == false && _isCommited is true)
                            {
                                try
                                {
                                    consumer.StoreOffset(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    _logger.LogError($"Store Offset error: {e.Error.Reason}");
                                }
                            }

                        }
                        catch (ConsumeException e)
                        {
                            _logger.LogError($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogInformation($"Consumer kafka got an exception error: {ex.Error.Reason}.\n Error message: {ex.Message}");
                }
                catch (TopicPartitionOffsetException ex)
                {
                    _logger.LogError($"Kafka error: {ex.Error.Reason}.\n Error message: {ex.Message}");
                }
                catch (KafkaException ex)
                {
                    if (ex?.Error == ErrorCode.Local_State)
                        _logger.LogWarning($"The message will be processed again by another consumer in the group. Ignore this warning\n {ex.Message}");
                    else
                        _logger.LogError($"Kafka error: {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    _logger.LogCritical($"{typeof(KafkaConsumer)}: {consumer.Name} has a critical error {ex.Message}");
                }
                finally
                {
                    _logger.LogInformation($"Closing consumer");
                    consumer?.Close();
                    consumer?.Dispose();
                }
            }
        }
    }
}
