using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ExemploKafkaDotNetCore
{
    class Program
    {
        //Exercicios:
        //1- Produtor gerando msgs a partir de arquivos
        //2- Colocar Command Line Parser https://github.com/commandlineparser/commandline
        //3- Consumidor salvando mensagem em base de dados
        public static void VerificaArgumentos(string[] args)
        {
            //Environment.Exit é importante quando você quer terminar a execução de uma aplicação console no .Net Core, informando ao sistema operacional o código de finalização. 
            //Normalmente "Exit Codes" são números inteiros, onde os negativos representam que algo errado aconteceu com a aplicação (exceptions).
            //-1 é o número negativo mais comum para Exit Code de erros.
            //Números positivos geralmente são indicativos de execuções com sucesso. 
            //Os números mais comuns são 0 e 1 para sucesso na aplicação.
            if (string.IsNullOrEmpty(args[1]) || (!args[1].Equals("--produtor") && !args[1].Equals("--consumidor")))
            {
                Console.WriteLine("Necessário informar o modo de uso: --consumidor ou --produtor");
                Console.WriteLine("Pressione ENTER para sair");
                Console.ReadLine();
                Environment.Exit(0);
            }
        }
        static void Main(string[] args)
        {

            Console.WriteLine("######  #     #    #                     ");
            Console.WriteLine("#     #  #   #     #         ##   #####  ");
            Console.WriteLine("#     #   # #      #        #  #  #    # ");
            Console.WriteLine("#     #    #       #       #    # #####  ");
            Console.WriteLine("#     #   # #      #       ###### #    # ");
            Console.WriteLine("#     #  #   #     #       #    # #    # ");
            Console.WriteLine("######  #     #    ####### #    # #####");
            Console.WriteLine("\n");

            VerificaArgumentos(args);

            Console.WriteLine("#     #                                #    #                             ");
            Console.WriteLine("#     # ###### #      #       ####     #   #    ##   ###### #    #   ##   ");
            Console.WriteLine("#     # #      #      #      #    #    #  #    #  #  #      #   #   #  #  ");
            Console.WriteLine("####### #####  #      #      #    #    ###    #    # #####  ####   #    # ");
            Console.WriteLine("#     # #      #      #      #    #    #  #   ###### #      #  #   ###### ");
            Console.WriteLine("#     # #      #      #      #    #    #   #  #    # #      #   #  #    # ");
            Console.WriteLine("#     # ###### ###### ######  ####     #    # #    # #      #    # #    # ");

            if (!string.IsNullOrEmpty(args[1]) && args[1] == "--consumidor")
            {
                LoopConsumo();
            }
            else if (!string.IsNullOrEmpty(args[1]) && args[1] == "--produtor")
            {
                LoopProdutor();
            }

        }

        public static void LoopConsumo()
        {
            //TODO: tratamento de registros atualizados - inicio do offset (offset management)
            var config = GetConsumerConfigs();

            using (var c = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                c.Subscribe("hello-kafka");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };


                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Mensagem consumida '{cr.Value}' em: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Erro: {e.Message}");
                        c.Close();
                        break;
                    }
                }



            }
            Console.WriteLine("Pressione enter para terminar");
            Console.ReadLine();
        }


        public static void LoopProdutor()
        {
            var config = GetProducerConfigs();

            Action<DeliveryReport<Null, string>> handler = r =>
            Console.WriteLine(!r.Error.IsError
                ? $"Entregue msg {r.Value} na partição { r.TopicPartition} offset {r.TopicPartitionOffset}"
                : $"Erro: {r.Error.Reason}");

            Console.WriteLine("Digite a mensagem que deseja enviar e tecle enter em seguida para enviar");
            Console.WriteLine("Para parar o envio, digite 'sair'");
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                while (true)
                {
                    var input = Console.ReadLine();
                    if (input.Equals("sair", StringComparison.OrdinalIgnoreCase))
                    {
                        break;
                    }
                    try
                    {

                        p.Produce("hello-kafka", new Message<Null, string> { Value = input }, handler);

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Erro no envio da msg: {e.Message}");
                    }
                }

            }
        }

        public static ProducerConfig GetProducerConfigs()
        {
            var config = new ProducerConfig();
            config.BootstrapServers = "<endereço servidor aqui>:9092";
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            config.SaslUsername = "<confluent cloud API Key>";
            config.SaslPassword = "<confluent cloud API Secret>";
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }

        public static ConsumerConfig GetConsumerConfigs()
        {
            var config = new ConsumerConfig();
            config.GroupId = "primeiro-grupo";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.BootstrapServers = "<endereço servidor aqui>:9092";
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            config.SaslUsername = "<confluent cloud API Key>";
            config.SaslPassword = "<confluent cloud API Secret>";
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }

    }
}
