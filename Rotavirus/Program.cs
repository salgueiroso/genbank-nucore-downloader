using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Knapcode.TorSharp;

namespace Gerlane
{
    class Program
    {

        static readonly string sufixo_out = "out.csv";
        static readonly char cs = ';';
        static readonly int ret_max = 200;
        static readonly int quantidadeRequestsDados = 20;
        static readonly object lockPerc = new object();
        static readonly Dictionary<string, FileStream> streamFiles = new Dictionary<string, FileStream>();

        static readonly TorSharpSettings settings = new TorSharpSettings
        {
            ZippedToolsDirectory = Path.Combine(Path.GetTempPath(), "TorZipped"),
            ExtractedToolsDirectory = Path.Combine(Path.GetTempPath(), "TorExtracted"),
            PrivoxyPort = 1337,
            TorSocksPort = 1338,
            TorControlPort = 1339,
            TorControlPassword = "foobar"
        };


        static readonly TorSharpProxy proxy = new TorSharpProxy(settings);

        static async Task Main(string[] args)
        {

            await new TorSharpToolFetcher(settings, new HttpClient()).FetchAsync();

            var p = new Program();

            Console.CancelKeyPress += delegate
            {
                p.FlushAll();

                Environment.Exit(0);
            };

            try
            {
                await p.Run();
            }
            finally
            {
                p.FlushAll();
                Console.WriteLine();
                Console.WriteLine("Processamento finalizado");
                Console.ReadLine();
            }
        }

        private void FlushAll()
        {
            foreach (var fs in streamFiles.Values)
            {
                fs.Flush();
                fs.Close();
            }
        }

        public async Task Run()
        {
            Console.WriteLine("===============================>>Processando<<==================================");
            Console.WriteLine("Obs.: Os arquivos de saída conterão as seguintes colunas:");
            Console.WriteLine("Accension | Collection Date | Segment | Region | Strain/Isolate");
            Console.WriteLine();

            var arquivos = FindFiles();
            Console.WriteLine($"{arquivos.Count()} arquivos encontrados:");

            await proxy.ConfigureAndStartAsync();

            foreach (var arquivo in arquivos)
                Console.WriteLine($"* {arquivo}");

            Console.WriteLine();

            foreach (var arquivo in arquivos)
            {
                Console.WriteLine("================================================================================");
                Console.WriteLine($"Processando arquvo '{arquivo}'");
                var linhas = CarregarLinhasPendentes(arquivo);

                var arquivoSaida = ObterNomeArquivoSaida(arquivo);

                streamFiles.Add(arquivoSaida, new FileStream(arquivoSaida, FileMode.Append));

                var gruposAccNum = AgruparPorQuantidade(ret_max, linhas);


                var items = new List<Item>();
                var total = gruposAccNum.SelectMany(g => g).Count();
                Console.WriteLine($"{total} registros encontrados..");
                for (int i = 0; i < gruposAccNum.Count(); i++)
                {
                    var grupoAccNum = gruposAccNum.ElementAt(i);

                    var index = (i * ret_max) + grupoAccNum.Count();

                    items.AddRange(await RequestAndTranslateGINumber(grupoAccNum));

                    var perc = (((decimal)index) / total) * 100m;
                    perc = Math.Truncate(perc);
                    WriteSameLine($"Obtidos GIs dos accession numbers: {index}/{total} ({perc}%)");
                }

                Console.WriteLine();

                var itemsGroups = AgruparPorQuantidade(quantidadeRequestsDados, items);
                total = items.Count;
                var step = 0;
                foreach (var grupo in itemsGroups)
                {
                    grupo.AsParallel().ForAll(item =>
                    {

                        try
                        {
                            RequestAndSaveItem(item, arquivoSaida).GetAwaiter().GetResult();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Console.WriteLine(ex.InnerException?.Message);
                            throw ex;
                        }
                        finally
                        {
                            ShowGetPercent(ref total, ref step, item.Accension);
                        }


                    });
                }

                Console.WriteLine();
                Console.WriteLine($"Processamento do arquivo '{arquivo}' finalizado!");
            }
        }

        private async Task<IEnumerable<Item>> RequestAndTranslateGINumber(IEnumerable<string> accessions, int exceptTime = 0)
        {
            string body = "";
            try
            {


                var handler = new HttpClientHandler
                {
                    Proxy = new WebProxy(new Uri("http://localhost:" + settings.PrivoxyPort))
                };

                using (var client = new HttpClient(handler))
                {


                    ConfigureHttpClient(client);

                    var strkeys = accessions.Select(x => x + "[accn]").Aggregate((x1, x2) => x1 + "+OR+" + x2);
                    var url = $"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nuccore&term={strkeys}&usehistory=y&retmax={ret_max}";


                    var response = await client.GetAsync(url);

                    var l = response.Content.Headers.ToList();

                    if (!response.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Retornou erro {response.StatusCode} - {response.ReasonPhrase}");
                        Console.ReadLine();
                        throw new Exception(response.ReasonPhrase);
                    }

                    body = await response.Content.ReadAsStringAsync();

                    XmlDocument doc = new XmlDocument();
                    doc.PreserveWhitespace = false;
                    doc.LoadXml(body);

                    var items = new List<Item>();

                    var elementListaId = doc.GetElementsByTagName("IdList")[0].ChildNodes;
                    for (int g = 0; g < elementListaId.Count; g++)
                    {
                        var id = elementListaId.Item(g).InnerText;

                        items.Add(new Item
                        {
                            Id = id
                        });
                    }

                    return items;

                }
            }
            catch (Exception ex)
            {
                exceptTime++;

                Console.WriteLine();
                Console.WriteLine($"Falha ao obter numeros GI (Tentativa {exceptTime})");
                Console.WriteLine($"Motivo: {ex.GetFullMessage()}");
                Console.WriteLine($"Dados retornados: {body}");

                if (exceptTime >= 30)
                    throw ex;


                Console.WriteLine($"Tentando novamente");
                if (body?.Contains("blocked") == true)
                    await proxy.GetNewIdentityAsync();
                return await RequestAndTranslateGINumber(accessions, exceptTime);
            }
        }

        private async Task RequestAndSaveItem(Item item, string arquivoSaida, int exceptTime = 0)
        {
            var body = "";
            try
            {
                var handler = new HttpClientHandler
                {
                    Proxy = new WebProxy(new Uri("http://localhost:" + settings.PrivoxyPort))
                };
                using (var client = new HttpClient(handler))
                {
                    ConfigureHttpClient(client);
                    var response = await client.GetAsync($"https://www.ncbi.nlm.nih.gov/sviewer/viewer.fcgi?id={item.Id}&db=nuccore&report=genbank&conwithfeat=on&hide-cdd=on&retmode=txt&withmarkup=on&tool=portal&log$=seqview&maxdownloadsize=1000000");
                    if (!response.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Item {item.Id} retornou erro {response.StatusCode} - {response.ReasonPhrase}");
                        Console.ReadLine();
                        throw new Exception(response.ReasonPhrase);
                    }

                    body = await response.Content.ReadAsStringAsync();

                    var accessionMatch = Regex.Match(body, "ACCESSION +(.+)");
                    if (accessionMatch.Success)
                        item.Accension = accessionMatch.Groups[1].Value.Trim();

                    var collectionDateMatch = Regex.Match(body, "/collection_date=\"(.+)\"");
                    if (collectionDateMatch.Success)
                        item.CollectionDate = collectionDateMatch.Groups[1].Value.Trim();

                    var countryMatch = Regex.Match(body, "/country=\"(.+)(:(.+))?\"");
                    if (countryMatch.Success)
                        item.Country = countryMatch.Groups[1].Value.Trim();

                    var regionMatch = Regex.Match(body, "/country=\"(.+)(:(.+))\"");
                    if (regionMatch.Success)
                        item.Region = regionMatch.Groups[3].Value.Trim();

                    var strainMatch = Regex.Match(body, "/strain=\"(.+)\"");
                    if (strainMatch.Success)
                        item.StrainIsolate = strainMatch.Groups[1].Value.Trim();

                    if (string.IsNullOrWhiteSpace(item.StrainIsolate))
                    {
                        var isolateMatch = Regex.Match(body, "/isolate=\"(.+)\"");
                        if (isolateMatch.Success)
                            item.StrainIsolate = isolateMatch.Groups[1].Value.Trim();
                    }

                    if (string.IsNullOrWhiteSpace(item.Region))
                        item.Region = DescobrirEstadoDoStrain(item.StrainIsolate);

                    var line = string.Join(cs, EscapeField(item.Accension), EscapeField(item.CollectionDate), EscapeField(item.Segment), EscapeField(item.Region), EscapeField(item.StrainIsolate));

                    AppendAllText(arquivoSaida, line + Environment.NewLine);
                }
            }
            catch (Exception ex)
            {
                exceptTime++;

                var itemCode = !string.IsNullOrWhiteSpace(item.Accension) ? item.Accension : item.Id;

                Console.WriteLine();
                Console.WriteLine($"Falha na tentativa {exceptTime} do item {itemCode}");
                Console.WriteLine($"Motivo: {ex.GetFullMessage()}");

                if (exceptTime >= 30)
                    throw ex;

                Console.WriteLine($"Tentando novamente {itemCode}");
                if (body?.Contains("blocked") == true)
                    await proxy.GetNewIdentityAsync();
                await RequestAndSaveItem(item, arquivoSaida, exceptTime);

            }
        }

        private string DescobrirEstadoDoStrain(string dado)
        {
            if (string.IsNullOrWhiteSpace(dado))
                return null;

            var uf = dado.Substring(0, 2);


            var dict = new Dictionary<string, string>() {
                {"ac", "Acre" },
                {"al", "Alagoas" },
                {"am", "Amazonas" },
                {"ap", "Amapá" },
                {"ba", "Bahia" },
                {"ce", "Ceará" },
                {"df", "Distrito Federal" },
                {"es", "Espírito Santo" },
                {"go", "Goiás" },
                {"ma", "Maranhão" },
                {"mt", "Mato Grosso" },
                {"ms", "Mato Grosso do Sul" },
                {"mg", "Minas Gerais" },
                {"pa", "Pará" },
                {"pb", "Paraíba" },
                {"pr", "Paraná" },
                {"pe", "Pernambuco" },
                {"pi", "Piauí" },
                {"rj", "Rio de Janeiro" },
                {"rn", "Rio Grande do Norte" },
                {"rs", "Rio Grande do Sul" },
                {"ro", "Rondônia" },
                {"rr", "Roraima" },
                {"sc", "Santa Catarina" },
                {"sp", "São Paulo" },
                {"se", "Sergipe" },
                {"to", "Tocantins" }
            };

            if (!dict.TryGetValue(uf, out var estado))
            {
                var ufMatch = Regex.Match(dado, "BRA/(.{2})");
                if (ufMatch.Success)
                {
                    var extraido = ufMatch.Groups[1].Value.Trim().ToLower();

                    dict.TryGetValue(extraido, out estado);
                }
            }

            return estado;
        }

        private string EscapeField(string data)
        {
            data = data?.Replace("\"", "\"\"");
            data = data?.Replace($"{cs}", $"\"{cs}\"");
            return data;
        }

        private IEnumerable<IEnumerable<T>> AgruparPorQuantidade<T>(int quantidade, IEnumerable<T> lista)
        {
            var grupo = new List<IEnumerable<T>>();
            var tmpLista = lista;
            while (tmpLista.Count() != 0)
            {
                grupo.Add(tmpLista.Take(quantidade).ToList());
                tmpLista = tmpLista.Skip(quantidade).ToArray();
            }

            return grupo;
        }

        private void ShowGetPercent(ref int total, ref int step, string accesionNumber)
        {
            lock (lockPerc)
            {
                step++;
                var perc = (((decimal)step) / total) * 100m;
                perc = Math.Truncate(perc);

                WriteSameLine($"Dados do {accesionNumber} obtidos: {step}/{total} ({perc}%)");
            }
        }

        private void AppendAllText(string arquivoSaida, string content)
        {
            lock (streamFiles[arquivoSaida])
            {
                var bytes = Encoding.Default.GetBytes(content.Trim() + Environment.NewLine);
                //var bytes = new UTF8Encoding(true).GetBytes(content.Trim() + Environment.NewLine);

                var fs = streamFiles[arquivoSaida];
                fs.Write(bytes, 0, bytes.Count());
                fs.Flush();
            }
        }

        private string ObterNomeArquivoSaida(string arquivo) => Path.ChangeExtension(arquivo, sufixo_out);

        private void WriteSameLine(string text)
        {
            Console.CursorLeft = 0;
            text = text.PadRight(Console.BufferWidth - 4);
            Console.Write(text);

        }

        private void ConfigureHttpClient(HttpClient client)
        {
            client.Timeout = TimeSpan.FromSeconds(30);
            var fkV = new Random().Next(DateTime.Now.Millisecond);
            client.DefaultRequestHeaders.Add("User-Agent", $"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 Aplubinho/{fkV}");
        }

        private IEnumerable<string> CarregarLinhasPendentes(string arquivo)
        {
            IEnumerable<string> linhas = File.ReadAllLines(arquivo).AsParallel().Select(l => l.Split(new[] { cs, ',' })[0]).ToList();

            var nmArqvSaida = ObterNomeArquivoSaida(arquivo);
            if (File.Exists(nmArqvSaida))
            {
                IEnumerable<string> linhasBaixadas = File.ReadAllLines(nmArqvSaida).AsParallel().Select(l => l.Split(cs)[0]).ToList();

                linhas = linhas.Where(l => !linhasBaixadas.Any(lb => lb == l)).ToList();
            }

            return linhas;
        }

        private string ObterDiretorioBasePadrao()
        {
            var def = @"C:\temp";
            var prevfile = Path.Combine(Environment.CurrentDirectory, "defaultDir.txt");

            if (File.Exists(prevfile))
                def = File.ReadAllText(prevfile).Trim();

            return def;
        }

        private void SalvarDiretorioBasePadrao(string content)
        {
            var prevfile = Path.Combine(Environment.CurrentDirectory, "defaultDir.txt");


            File.WriteAllText(prevfile, content.Trim());
        }

        public IEnumerable<string> FindFiles()
        {
            var diretorioBaseDef = ObterDiretorioBasePadrao();

            Console.Write($"Informe o diretório dos arquivos .csv [{diretorioBaseDef}]: ");
            var diretorioBase = Console.ReadLine();

            while (!string.IsNullOrWhiteSpace(diretorioBase) && !Directory.Exists(diretorioBase))
            {
                Console.Write("Informe o diretório dos arquivos .csv: ");
                diretorioBase = Console.ReadLine();
            }

            diretorioBase = !string.IsNullOrWhiteSpace(diretorioBase) ? diretorioBase : diretorioBaseDef;

            SalvarDiretorioBasePadrao(diretorioBase);

            return Directory
                .GetFiles(diretorioBase, "*.csv", SearchOption.AllDirectories)
                .Where(i => i.ToLower().EndsWith(sufixo_out) == false)
                .ToList();
        }
    }



    public class Item
    {
        public string Accension { get; set; }
        public string Id { get; set; }
        public string CollectionDate { get; set; }
        public string Segment { get; set; }
        public string Country { get; set; }
        public string Region { get; set; }
        public string StrainIsolate { get; set; }
    }

    public static class ExceptionExtension
    {
        public static string GetFullMessage(this Exception source)
        {
            var msg = source.Message;
            Exception ex = source.InnerException;
            while (ex != null)
            {
                msg += " " + ex.Message.Trim();
                ex = ex.InnerException;
            }

            return msg + Environment.NewLine;
        }
    }
}
