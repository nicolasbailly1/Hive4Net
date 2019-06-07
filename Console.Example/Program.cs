using System;
using System.Collections.Generic;
using System.Text;
using Hive4Net;
using Thrift.Transports.Client;

namespace Console.Example
{
    class Program
    {
        static void Main(string[] args)
        {
            string userName = "xxx";
            string password = "****";
            var byteArray = Encoding.ASCII.GetBytes($"{userName}:{password}");
            Dictionary<string, string> customHeaders = new Dictionary<string, string>();
            customHeaders.Add("Authorization", $"Basic {Convert.ToBase64String(byteArray)}");
            //Uri uri = new Uri("https://powerbiintrahdiiq.azurehdinsight.net/cliservice");
            Uri uri = new Uri("http://localhost:10001/cliservice");
            var transport = new THttpClientTransport(uri, customHeaders);

            System.Console.WriteLine("Open connection");
            Hive hive = new Hive(transport);
            hive.OpenAsync().Wait();
            System.Console.WriteLine("Get Cursor");
            var cursor = hive.GetCursorAsync().Result;
            System.Console.WriteLine("Execute query");
            cursor.ExecuteAsync("SHOW TABLES").Wait();
            var result = cursor.FetchManyAsync(100).Result;
            if (!result.IsEmpty())
            {
                System.Console.WriteLine("-----------Début-------------");
                foreach (var row in result)
                {
                    System.Console.Write("Row - ");
                    var dict = row as IDictionary<string, object>;
                    foreach (var key in dict.Keys)
                    {
                        System.Console.WriteLine($"{key} : {dict[key].ToString()}");
                    }
                }
                System.Console.WriteLine("-----------Fin-------------");
            }
            else
            {
                System.Console.WriteLine("No result");
            }
            System.Console.WriteLine("Finished");
        }
    }
}
