using KafkaClient;
using Microsoft.Extensions.Logging;

using ILoggerFactory loggerFactory = LoggerFactory.Create(bulder => bulder.AddConsole());

KafkaConsumer consumer = new KafkaConsumer(loggerFactory.CreateLogger<KafkaConsumer>(), new KafkaHandler());

await consumer.Consume(CancellationToken.None);
