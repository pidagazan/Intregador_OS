using System.IO.Pipes;
using System.Text;
using System.Diagnostics;
using System.Linq.Expressions;

namespace Emisor
{
    class Program
    {
        static bool verificar_dni(string dni)
        {
            /// Formato DNI = XX.XXX.XXX
            bool resultado = true;
            string[] arreglo_string_dni = dni.Split(".");
            
            if (arreglo_string_dni.Length != 3)
            {
                resultado = false;
                return resultado;
            }

            for (int i = 0; i < arreglo_string_dni.Length; i++)
            {
                int triada_int = Convert.ToInt32(arreglo_string_dni[i]);
                if ( triada_int > 999 || triada_int < 0)
                {
                    resultado = false;
                    return resultado;
                }
            }
            return resultado;
        }
        static bool verificar_tarjeta(string tarjeta)
        {
            ///FORMATO TARJETA = XXXX.XXXX.XXXX.XXXX
            bool resultado = true;
            string[] arreglo_tarjeta = tarjeta.Split(".");
            if (arreglo_tarjeta.Length != 4)
            {
                resultado = false;
                return resultado;
            }
            for (int i = 0; i < arreglo_tarjeta.Length; i++)
            {
                int cuarteto_int = Convert.ToInt32(arreglo_tarjeta[i]);
                if ( cuarteto_int > 9999 || cuarteto_int < 0) /// TRUE siempre que sea menor a 10000 y mayor a 0
                {
                    resultado = false;
                    return resultado;
                }
            }
            
            return resultado;
        }

        
        static bool verificar_nombre(string nombre)
        {
            bool resultado = true;
            if (nombre == null)
            {
                resultado = false;
            }
            else
            {
                foreach (char letra in nombre)
                {
                    if (!Char.IsLetter(letra))
                    {
                        resultado = false;
                        return resultado;
                    }
                }
            }
            return resultado;
        }
        
        static byte[] obtener_bytes_de_entero(Int32 entero)
        {
            byte[] bytes_entero = BitConverter.GetBytes(entero);
            
            return bytes_entero;
        }
        

        static async Task Main()
        {           
            Process proc = Process.GetCurrentProcess();
            Console.WriteLine("Información del proceso");
            Console.WriteLine("PID: {0}",proc.Id);
            Console.WriteLine("Memoria (bytes): {0}",proc.WorkingSet64);
            Console.WriteLine("Tiempo CPU: {0}",proc.TotalProcessorTime);
            
            Semaphore semaforo_compartido = new Semaphore(0,1,"semaforo_compartido");
            
            Console.WriteLine("Inicializando cliente");

            var pipe_mensajeria = new NamedPipeClientStream(".","pipe_servidor_mensajeria");
            
            var pipe_stream = new NamedPipeClientStream(".","pipe_servidor_stream");

            try
            {

                pipe_mensajeria.Connect();
                pipe_stream.Connect();
            }
            
            catch (Exception e)
            {
                Console.WriteLine("Excepcion encontrada");
                Console.WriteLine(e);
                Environment.Exit(1);
            }
            
            
            var writer = new StreamWriter(pipe_mensajeria) {AutoFlush = true};
            var reader = new StreamReader(pipe_mensajeria);
            
            string? ver_conectado = await reader.ReadLineAsync();
            Console.WriteLine(ver_conectado);
 
            while (true)
            {
            try{
                string? pedido_mensaje = Console.ReadLine();
                await writer.WriteLineAsync(pedido_mensaje);

                string? respuesta = await reader.ReadLineAsync();
                Console.WriteLine(respuesta);
                
                if (pedido_mensaje != null)
                {
                    if (pedido_mensaje == "SALIR")
                    {
                        Thread.Sleep(1000);
                        semaforo_compartido.WaitOne();
                        Console.WriteLine("Cerrando programa...");
                        Console.WriteLine("Cerrando pipes");
                        pipe_mensajeria.Dispose();
                        pipe_stream.Dispose();
                        semaforo_compartido.Release();

                        Thread.Sleep(1000);
                        Environment.Exit(0);
                    }
                    if (pedido_mensaje == "registro")
                    {
                        bool inputs_verificados = false;
                        string? nombre;
                        string? dni;
                        string? tarjeta;
                        using var tiempo_max = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        do 
                        {
                                if (tiempo_max.IsCancellationRequested)
                                {
                                    throw new Exception("Se cerró la conexión con el servidor");
                                }
                                                               
                                Console.WriteLine("Ingrese un Nombre"); 
                                nombre = Console.ReadLine();
                        
                                Console.WriteLine("Ingrese un DNI"); 
                                dni = Console.ReadLine();

                                Console.WriteLine("Ingrese su tarjeta"); 
                                tarjeta = Console.ReadLine();

                                if (!verificar_dni(dni) || !verificar_nombre(nombre) || !verificar_tarjeta(tarjeta))
                                {
                            
                                Console.WriteLine("Algunos de los datos ingresado no son validos");
                                Console.WriteLine("Formato:");
                                Console.WriteLine("DNI y tarjeta deben ser ingresados con puntos, solo de aceptan tarjetas de 16 dígitos:");
                                Console.WriteLine("DNI: XXX.XXX.XXX");
                                Console.WriteLine("Tarjeta:XXXX.XXXX.XXXX.XXXX");

                                }
                                else        
                                {
                                    inputs_verificados = true;
                                }
                        }
                        while (!inputs_verificados);
                        
                        /// ENVIO DE NOMBRE
                        byte[] arreglo_byte_nombre = Encoding.UTF8.GetBytes(nombre);
                        byte[] largo_arreglo_nombre = obtener_bytes_de_entero(arreglo_byte_nombre.Length);
                                                
                        await pipe_stream.WriteAsync(largo_arreglo_nombre,0,4);
                        await pipe_stream.WriteAsync(arreglo_byte_nombre);


                        
                        /// ENVIO DE DNI
                        byte[] arreglo_byte_dni = Encoding.UTF8.GetBytes(dni);
                        byte[] largo_arreglo_dni = obtener_bytes_de_entero(arreglo_byte_dni.Length);

                        await pipe_stream.WriteAsync(largo_arreglo_dni,0,4);
                        await pipe_stream.WriteAsync(arreglo_byte_dni);

                        
                        

                        /// ENVIO DE TARJETA
                        byte[] arreglo_byte_tarjeta = Encoding.UTF8.GetBytes(tarjeta);
                        byte[] largo_arreglo_tarjeta = obtener_bytes_de_entero(arreglo_byte_tarjeta.Length);

                        await pipe_stream.WriteAsync(largo_arreglo_tarjeta,0,4);
                        await pipe_stream.WriteAsync(arreglo_byte_tarjeta);
                        
                    Console.Clear();
                    Console.WriteLine(await reader.ReadLineAsync());
                    Thread.Sleep(3000);
                    
                }
            }}
            catch (Exception e)
            {
                Console.WriteLine("Excepcion encontrada");
                Console.WriteLine("Cerrando programa");
                pipe_mensajeria.Dispose();
                pipe_mensajeria.Dispose();
                Environment.Exit(1);
            }    
            
        }}}
            
                
                }
                