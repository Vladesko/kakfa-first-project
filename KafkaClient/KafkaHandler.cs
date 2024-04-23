using Confluent.Kafka;
using KafkaClient.Models;
using System.Diagnostics;
using System.Xml.Serialization;

namespace KafkaClient
{
    public class KafkaHandler : IKafkaHandler
    {
        private static readonly XmlSerializer serializer = new(typeof(Book));
        private List<Book> books = new List<Book>();
        public Task<bool> MessageHandle(ConsumeResult<Ignore, string> consumeResult, CancellationToken cancellationToken)
        {
            try
            {
                var message = consumeResult.Message.Value;
                using (var reader = new StringReader(message))
                {
                    if(reader is not null)
                    {
                        var messageData = serializer.Deserialize(reader) as Book;
                        books.Add(messageData);
                    }
                }
                return Task.FromResult(true);
            }
            catch (Exception exception)
            {
                Debug.WriteLine(exception, exception.Message);
                return Task.FromResult(false);
            }
        }
    }
}
