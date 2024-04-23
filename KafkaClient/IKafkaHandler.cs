using Confluent.Kafka;

namespace KafkaClient
{
    public interface IKafkaHandler
    {
        Task<bool> MessageHandle(ConsumeResult<Ignore, string> consumeResult, CancellationToken cancellationToken);
    }
}
