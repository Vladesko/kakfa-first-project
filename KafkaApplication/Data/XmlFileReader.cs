using System.Text;

namespace KafkaApplication.Data
{
    public class XmlFileReader : IXmlFileReader
    {
        public async Task<string> ReadXml(string path)
        {
            var filePath = Path.GetFullPath(path);
            return await File.ReadAllTextAsync(filePath, encoding: Encoding.UTF8);
        }
    }
}
