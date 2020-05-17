using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace Gerlane
{
    class Program
    {

        static readonly string sufixo_out = "out.csv";
        static readonly int ret_max = 10000;
        static void Main(string[] args)
        {
            try
            {
                new Program().Run().GetAwaiter().GetResult();
            }
            finally
            {
                Console.ReadLine();
            }
        }

        public async Task Run()
        {
            foreach (var arquivo in FindFiles())
            {
                Console.WriteLine("================================================================================");
                Console.WriteLine($"Processando arquvo '{arquivo}'");
                var linhas = CarregarLinhasPendentes(arquivo);


                var gruposAccNum = new List<IEnumerable<(int index, string accession)>>();

                var tmpLinhas = linhas;
                while (tmpLinhas.Count() != 0)
                {
                    gruposAccNum.Add(tmpLinhas.Take(ret_max).ToList());
                    tmpLinhas = tmpLinhas.Skip(ret_max).ToArray();
                }



                var items = new List<(int indice, string accension, string id, string collection_date, string segment, string country, string region, string strain_isolate)>();
                var total = gruposAccNum.SelectMany(g => g).Count();
                Console.WriteLine($"{total} registros encontrados..");
                for (int i = 0; i < gruposAccNum.Count; i++)
                {
                    var grupoAccNum = gruposAccNum[i];
                    using (var client = new HttpClient())
                    {
                        ConfigureHttpClient(client);

                        var strkeys = grupoAccNum.Select(x => x.accession + "[accn]").Aggregate((x1, x2) => x1 + "+OR+" + x2);
                        var url = $"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nuccore&term={strkeys}&usehistory=y&retmax={ret_max}";
                        var response = await client.GetAsync(url);
                        if (!response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"Interation {i} : Item {strkeys} retornou erro {response.StatusCode} - {response.ReasonPhrase}");
                            Console.ReadLine();
                            throw new Exception(response.ReasonPhrase);
                        }

                        var body = await response.Content.ReadAsStringAsync();

                        XmlDocument doc = new XmlDocument();
                        doc.PreserveWhitespace = false;
                        doc.LoadXml(body);

                        var elementListaId = doc.GetElementsByTagName("IdList")[0].ChildNodes;
                        for (int g = 0; g < elementListaId.Count; g++)
                        {
                            var index = (i * ret_max) + g;

                            var accension = grupoAccNum.ElementAt(g).accession;
                            var id = elementListaId.Item(g).InnerText;

                            items.Add((index, accension, id, "", "", "", "", ""));

                            var perc = (((decimal)index) / total) * 100m;
                            perc = Math.Truncate(perc);
                            WriteSameLine($"Obtendo IDs dos accession numbers: {index}/{total} ({perc}%)");
                        }

                    }

                }

                Console.WriteLine();

                for (int i = 0; i < items.Count; i++)
                {
                    var item = items[i];

                    var perc = (((decimal)i) / items.Count) * 100m;
                    perc = Math.Truncate(perc);

                    WriteSameLine($"Obtendo dados do accession {item.accension}: {i}/{items.Count} ({perc}%)");

                    using (var client = new HttpClient())
                    {
                        ConfigureHttpClient(client);
                        var response = await client.GetAsync($"https://www.ncbi.nlm.nih.gov/sviewer/viewer.fcgi?id={item.id}&db=nuccore&report=genbank&conwithfeat=on&hide-cdd=on&retmode=txt&withmarkup=on&tool=portal&log$=seqview&maxdownloadsize=1000000");
                        if (!response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"Interation {i} : Item {item.id} retornou erro {response.StatusCode} - {response.ReasonPhrase}");
                            Console.ReadLine();
                            throw new Exception(response.ReasonPhrase);
                        }

                        var body = await response.Content.ReadAsStringAsync();

                        var accessionMatch = Regex.Match(body, "ACCESSION +(.+)");
                        if (accessionMatch.Success)
                            item.accension = accessionMatch.Groups[1].Value.Trim();

                        var collectionDateMatch = Regex.Match(body, "/collection_date=\"(.+)\"");
                        if (collectionDateMatch.Success)
                            item.collection_date = collectionDateMatch.Groups[1].Value.Trim();

                        var countryMatch = Regex.Match(body, "/country=\"(Brazil)\"");
                        if (countryMatch.Success)
                            item.country = countryMatch.Groups[1].Value.Trim();

                        var regionMatch = Regex.Match(body, "/country=\"Brazil:(.+)\"");
                        if (regionMatch.Success)
                            item.region = regionMatch.Groups[1].Value.Trim();

                        var strainMatch = Regex.Match(body, "/strain=\"(.+)\"");
                        if (strainMatch.Success)
                            item.strain_isolate = regionMatch.Groups[1].Value.Trim();

                        if (string.IsNullOrWhiteSpace(item.strain_isolate))
                        {
                            var isolateMatch = Regex.Match(body, "/isolate=\"(.+)\"");
                            if (isolateMatch.Success)
                                item.strain_isolate = isolateMatch.Groups[1].Value.Trim();
                        }

                        var arquivoSaida = ObterNomeArquivoSaida(arquivo);

                        var line = $"{item.accension},{item.collection_date},{item.segment},{item.strain_isolate}";

                        File.AppendAllText(arquivoSaida, line + Environment.NewLine);
                    }
                }

            }
        }

        private string ObterNomeArquivoSaida(string arquivo) => Path.ChangeExtension(arquivo, sufixo_out);

        private void WriteSameLine(string text)
        {
            Console.CursorLeft = 0;
            Console.Write(text);
        }

        private void ConfigureHttpClient(HttpClient client)
        {
            client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36");
        }

        private IEnumerable<(int index, string accession)> CarregarLinhasPendentes(string arquivo)
        {
            IEnumerable<(int index, string accession)> linhas = File.ReadAllLines(arquivo).AsParallel().Select((l, i) => (i, l.Split(',')[0])).ToList();

            var nmArqvSaida = ObterNomeArquivoSaida(arquivo);
            if (File.Exists(nmArqvSaida))
            {
                IEnumerable<(int index, string accession)> linhasBaixadas = File.ReadAllLines(nmArqvSaida).AsParallel().Select((l, i) => (i, l.Split(',')[0])).ToList();

                linhas = linhas.Where(l => !linhasBaixadas.Any(lb => lb.index == l.index && lb.accession == l.accession)).ToList();
            }

            return linhas;
        }

        public IEnumerable<string> FindFiles()
        {
            //Console.Write("Informe o diretório dos arquivos .csv: ");
            //var diretorioBase = Console.ReadLine();

            //while (!string.IsNullOrWhiteSpace(diretorioBase) && !Directory.Exists(diretorioBase))
            //{
            //    Console.Write("Informe o diretório dos arquivos .csv: ");
            //    diretorioBase = Console.ReadLine();
            //}

            //diretorioBase = !string.IsNullOrWhiteSpace(diretorioBase) ? diretorioBase : Environment.CurrentDirectory;
            var diretorioBase = @"C:\temp";

            return Directory
                .GetFiles(diretorioBase, "*.csv", SearchOption.AllDirectories)
                .Where(i => i.ToLower().EndsWith(sufixo_out) == false)
                .ToList();
        }
    }
}
