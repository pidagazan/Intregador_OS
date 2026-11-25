using System.IO.Pipes;
using System.Text;
using System.Diagnostics;

namespace Receptor
{
    class ClienteCerrado : Exception
    {
        public ClienteCerrado(string message)
        : base(message)
        {
            
        }
    }
    class Program
    {
        static string enmascarar_tarjeta(string tarjeta_string)
        {
            /// FORMATO TARJETA = XXXX XXXX XXXX XXXX (16dig)
            string[] arreglo_cuarteto = tarjeta_string.Split(".");
            string ultimo_cuarteto = arreglo_cuarteto[3];
            string tarjeta_enmascarada = ("XXXX.XXXX.XXXX."+ultimo_cuarteto);
            
            return tarjeta_enmascarada;
        }
        static async Task Main()
        {
            Process proc = Process.GetCurrentProcess();
            Console.WriteLine("Información del proceso");
            Console.WriteLine("PID: {0}",proc.Id);
            Console.WriteLine("Memoria (bytes): {0}",proc.WorkingSet64);
            Console.WriteLine("Tiempo CPU: {0}",proc.TotalProcessorTime);
            
            Semaphore semaforo_compartido = new Semaphore(0,1,"semaforo_compartido");
            
            string path_log = @"./logs.txt";

            if (!File.Exists(path_log))
            {
                var archivo_log = File.Create(path_log);
                archivo_log.Close();
            }

            Console.WriteLine("Inicializando Servidor");

            var pipe_mensajeria = new NamedPipeServerStream("pipe_servidor_mensajeria",
                                                PipeDirection.InOut,1,PipeTransmissionMode.Message,
                                                System.IO.Pipes.PipeOptions.Asynchronous);
            
            var pipe_stream = new NamedPipeServerStream("pipe_servidor_stream",
                                                        PipeDirection.InOut,1,PipeTransmissionMode.
                                                        Byte,System.IO.Pipes.PipeOptions.Asynchronous);


            Console.WriteLine("Pipes inicializados, esperando conexión");

            try {
            pipe_mensajeria.WaitForConnection();
            pipe_stream.WaitForConnection();

            if (pipe_mensajeria.IsConnected & pipe_stream.IsConnected)
                {
                    Console.WriteLine("Pipes conectados...");
                    Console.WriteLine("Log inicializado en {0}",Directory.GetCurrentDirectory()+"/logs");
                }   
                }
            catch (Exception e)
            {
                Console.WriteLine("Excepcion encontrada");
                Console.WriteLine(e);
            }
            
            var writer = new StreamWriter(pipe_mensajeria) {AutoFlush = true};
            var reader = new StreamReader(pipe_mensajeria);

            await writer.WriteLineAsync("Mensaje del servidor, cliente conectado");
            try
            {
                
                while (true)
                {
                    /// verificación de desconexión
                    if (!(pipe_mensajeria.IsConnected & pipe_stream.IsConnected))
                    {
                        throw new ClienteCerrado(
                            "El cliente ha cortado la comunicacion de forma inesperada");
                    }

                    /// lectura de recibido
                    string? pedido_recibido = await reader.ReadLineAsync();
                    
                    if (!(pedido_recibido == "SALIR" || pedido_recibido == "registro"))
                    {
  
                        string respuesta = ("Mensaje del servidor: Ingrese la palabra 'registro' para iniciar la transmisión de datos o Ingresa la palabra SALIR, para finalizar los procesos");
                        await writer.WriteLineAsync(respuesta);
                    
                        
                    }
                    
                    if (pedido_recibido == "SALIR")
                    {
                        string respuesta = ("Mensaje del servidor: cerrando conexión");
                        await writer.WriteLineAsync(respuesta);
                        semaforo_compartido.WaitOne();
                        Console.WriteLine("Cesando comunicaciones");
                        Console.WriteLine("Cerrando pipes...");
                        pipe_mensajeria.Dispose();
                        pipe_stream.Dispose();
                        semaforo_compartido.Release();
                        Environment.Exit(0);

                    }
                    if (pedido_recibido == "registro")
                    {
                        try{
                        
                        string respuesta = ("Mensaje del servidor: Ingresando a modo registro");
                        await writer.WriteLineAsync(respuesta);

                        Console.WriteLine("Modo recepcion de registro:"); 
                        
                        byte[] buffer = new Byte[512];

                        using var tiempo_max = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        
                        /// RECEPCION DE NOMBRE                                                                     
                        
                        await pipe_stream.ReadExactlyAsync(buffer,0,4,tiempo_max.Token);
                        Int32 largo_arreglo_nombre = BitConverter.ToInt32(buffer);

                        await pipe_stream.ReadExactlyAsync(buffer,0,largo_arreglo_nombre,tiempo_max.Token);                   
                        string? nombre = Encoding.UTF8.GetString(buffer,0,largo_arreglo_nombre);
                        // FIN DE RECEPCION NOMBRE                                                          

                        /// RECEPCION DE DNI                                        
                        await pipe_stream.ReadExactlyAsync(buffer,0,4,tiempo_max.Token);                    
                        Int32 largo_arreglo_dni = BitConverter.ToInt32(buffer);

                        await pipe_stream.ReadExactlyAsync(buffer,0,largo_arreglo_dni,tiempo_max.Token);                    
                        string? dni = Encoding.UTF8.GetString(buffer,0,largo_arreglo_dni);
                        /// FIN DE RECEPCION DNI
                    
                        /// RECEPCION DE TARJETA
                        await pipe_stream.ReadExactlyAsync(buffer,0,4,tiempo_max.Token); 
                        Int32 largo_arreglo_tarjeta = BitConverter.ToInt32(buffer);                   
                    
                        await pipe_stream.ReadExactlyAsync(buffer,0,largo_arreglo_tarjeta,tiempo_max.Token);                    
                        string? tarjeta = Encoding.UTF8.GetString(buffer,0,largo_arreglo_tarjeta);
                        string? tarjeta_enmascarada = enmascarar_tarjeta(tarjeta);
                        /// FIN DE TARJETA
                        
                       
                            /// NO SE RECIBIO EN TIEMPo
                        
                        Console.WriteLine("Los datos recibidos fueron: nombre: {0}",nombre);
                        Console.WriteLine("Los datos recibidos fueron: dni: {0}",dni);
                        Console.WriteLine("Los datos recibidos fueron: tarjeta: {0}",tarjeta_enmascarada);
                        
                        ///Escritura a archivo log.txt
                        using (var stream_to_file = new FileStream(path_log,FileMode.Append,FileAccess.Write))
                        {
                        using (StreamWriter writer_to_file = new StreamWriter(stream_to_file))
                        {
                            string time_stamp_registro = DateTime.Now.ToString();

                            writer_to_file.WriteLine(time_stamp_registro + " " + dni + " " + nombre + " " + tarjeta_enmascarada);
                    
                            await writer.WriteLineAsync("Registro recibido de forma exitosa");
                        }
                        
                        }

                    }
                    catch (OperationCanceledException)
                        {
                            Console.WriteLine("El cliente no ha contestado a tiempo, cerrando conexión");
                            pipe_mensajeria.Dispose();
                            pipe_stream.Dispose();
                            Environment.Exit(1);
                        }
                }}
            }
            catch (ClienteCerrado)
            {
                Console.WriteLine("El Proceso cliente se ha cerrado de manera inesperada, cerrando proceso filtro.");
                pipe_mensajeria.Dispose();
                pipe_stream.Dispose();
                Thread.Sleep(3000);
                Environment.Exit(1);
            }

            catch (Exception)
            {
                Console.WriteLine("Se ha detectado un error fatal cerrando programa");
                pipe_mensajeria.Dispose();
                pipe_stream.Dispose();
                Thread.Sleep(3000);

                Environment.Exit(1);
                
            }
        }}}
        
