using Confluent.Kafka;
using KafkaApplication.Data;
using System.Diagnostics;

namespace KafkaApplication
{
    public class Producer(IXmlFileReader xmlReader, string path)
    {
        private readonly string path = path;
        private readonly string topic = "kafka-first-topic";
        private readonly IXmlFileReader xmlReader =xmlReader;

        public async Task SetMessage()
        {
            var config = new ProducerConfig();
            config.BootstrapServers = "localhost:9092";
            config.EnableSslCertificateVerification = false;
            config.SecurityProtocol = SecurityProtocol.Plaintext;

            using(var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var message = await xmlReader.ReadXml(path);
                    producer.Produce(topic, new Message<Null, string> { Value = message });
                    producer.Flush();
                }
                catch (ProduceException<Null, string> exception)
                {
                    Debug.WriteLine(exception, $"Producer exception: {exception.Message}");
                }
                catch (Exception exception)
                {
                    Debug.WriteLine(exception, exception.Message);
                }
            }
        }
    }
}
