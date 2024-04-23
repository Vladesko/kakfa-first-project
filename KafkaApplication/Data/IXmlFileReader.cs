namespace KafkaApplication.Data
{
    public interface IXmlFileReader
    {
        Task<string> ReadXml(string path);
    }
}
