using KafkaApplication;
using KafkaApplication.Data;

string xmlPath = @"D:\Обучение\Программирование\С#\C#\KafkaApplication\KafkaApplication\Data\books.xml";

var producer = new Producer(new XmlFileReader(), xmlPath);
await producer.SetMessage();

Console.WriteLine("Message has written to topic");
