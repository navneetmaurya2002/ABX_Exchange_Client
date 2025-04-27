using Microsoft.Extensions.Configuration;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace ABX_Exchange_Client
{
  public class Program
  {
    static ConfigValues? configValues;
    public const int PacketSize = 17;
    private List<TxnPacket> packets = new List<TxnPacket>();
    static async Task Main(string[] args)
    {

      // Setting Up Configuration To Read appsettings.json
      var config = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory()) // Needed to find appsettings.json
           .AddJsonFile("Appsettings.json", optional: false, reloadOnChange: true)
           .Build();
      configValues = new ConfigValues(config["ServerConfig:ServerIP"]!, int.Parse(config["ServerConfig:ServerPort"]!)); // Read the values from appsettings.json
      await new Program().RunAsync();
    }

    public async Task RunAsync()
    {

      var GetAllPackets = StreamAllPackets();

      var MissingPackets = FindMissingSequences(GetAllPackets);
      var missingPackets = await RequestMissingPacketsInParallel(MissingPackets);

      packets.AddRange(GetAllPackets);
      packets.AddRange(missingPackets!);
      packets = packets.OrderBy(p => p.Sequence).ToList();

      Console.WriteLine("\nAll packets received:");
      SavePacketsToFile("output.json");
      Console.ReadKey();

    }
    private void SavePacketsToFile(string filePath)
    {
      var options = new JsonSerializerOptions
      {
        WriteIndented = true // To Print in Different Lines
      };
      string jsonString = JsonSerializer.Serialize(packets, options);
      File.WriteAllText(filePath, jsonString);
      Console.WriteLine($"Saved {packets.Count} packets to {filePath}");
    }
    // To Request Missing Packets
    private async Task<List<TxnPacket?>> RequestMissingPacketsInParallel(List<int> missingSequences)
    {
      var tasks = missingSequences.Select(seq => Task.Run(() => RequestResendPacket(seq))).ToList();

      var results = await Task.WhenAll(tasks);
      return results.Where(p => p != null).ToList();
    }
    private TxnPacket? RequestResendPacket(int sequenceNumber)
    {
      try
      {
        using (var client = new TcpClient(configValues?.ServerIP!, configValues.ServerPort))
        using (var networkStream = client.GetStream())
        {
          // Send CallType 2 (Resend Packet)
          networkStream.Write(new byte[] { 2, (byte)sequenceNumber }, 0, 2);

          var buffer = new byte[PacketSize];
          int bytesRead = networkStream.Read(buffer, 0, PacketSize);

          if (bytesRead == PacketSize)
          {
            return ParsePacket(buffer);
          }
          else
          {
            Console.WriteLine($"Failed to retrieve packet with sequence {sequenceNumber}.");
            return null;
          }
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine($"Error retrieving packet {sequenceNumber}: {ex.Message}");
        return new TxnPacket();
      }
    }

    // To Find Which Packet is Missing
    private List<int> FindMissingSequences(List<TxnPacket> packets)
    {
      var sequences = packets
          .Select(p => p.Sequence)
          .Distinct()
          .OrderBy(seq => seq)
          .ToList();

      var missing = new List<int>();

      for (int i = 0; i < sequences.Count - 1; i++)
      {
        for (int missingSeq = sequences[i] + 1; missingSeq < sequences[i + 1]; missingSeq++)
        {
          missing.Add(missingSeq);
        }
      }

      return missing;
    }
    // Method to Get All Packets
    private List<TxnPacket> StreamAllPackets()
    {
      var packets = new List<TxnPacket>();
      try
      {
        using (TcpClient client = new TcpClient(configValues?.ServerIP!, configValues.ServerPort))
        {
          Console.WriteLine($"Connected to server at {configValues.ServerIP}:{configValues.ServerPort}");
          using (NetworkStream stream = client.GetStream())
          {
            stream.Write(new byte[] { 1, 0 }, 0, 2);
            var buffer = new byte[PacketSize];
            int bytesRead;
            while ((bytesRead = stream.Read(buffer, 0, PacketSize)) > 0)
            {
              if (bytesRead == PacketSize)
              {
                var packet = ParsePacket(buffer);
                packets.Add(packet);
              }
            }
          }
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine($"Error: {ex.Message}");
      }
      return packets;
    }

    private TxnPacket ParsePacket(byte[] buffer)
    {
      // Ensure big endian
      string symbol = Encoding.ASCII.GetString(buffer, 0, 4);
      string buySell = Encoding.ASCII.GetString(buffer, 4, 1);
      int quantity = BitConverter.ToInt32(buffer.Skip(5).Take(4).Reverse().ToArray(), 0);
      int price = BitConverter.ToInt32(buffer.Skip(9).Take(4).Reverse().ToArray(), 0);
      int sequence = BitConverter.ToInt32(buffer.Skip(13).Take(4).Reverse().ToArray(), 0);

      return new TxnPacket
      {
        TickerSymbol = symbol,
        BuySellIndicator = buySell,
        Quantity = quantity,
        Price = price,
        Sequence = sequence
      };
    }



    internal class ConfigValues
    {
      public ConfigValues(string? serverIP, int serverPort)
      {
        ServerIP = serverIP;
        ServerPort = serverPort;
      }

      public string? ServerIP { get; set; }
      public int ServerPort { get; set; }
    }
  }
  public class TxnPacket
  {
    public string? TickerSymbol { get; set; }
    public string? BuySellIndicator { get; set; }
    public int Quantity { get; set; }
    public int Price { get; set; }
    public int Sequence { get; set; }
  }
}
