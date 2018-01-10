using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using NewLife.Log;
using Microsoft.Extensions.Configuration;

namespace test1
{
    /*
    /// 然后分别每次写入16字节 64字节 256字节 1k 4k 16k大概这样,
    /// 比如每次写入64字节，不管多少线程，一起写入10亿次，然后算整体时间多少
    ///10亿除以时间，就是速度
    /// 多线程
    /// 你实际测试20~60秒左右就可以了，sw运行时间范围
    */
    class Program
    {
        /// <summary>
        /// test
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {

            XTrace.UseConsole(true, true);
            var p = Process.GetCurrentProcess();

            #region 创建10G文件及其内存映射文件

            XTrace.Log.Info($"EXP:随机读写");
            try
            {
                #region 配置文件初始化

                var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
             .AddEnvironmentVariables();
                var configuration = builder.Build();
                var settings = new MySettings();
                configuration.Bind(settings);
                var moduleSettings = new MyModuleSettings();

                configuration.GetSection("SectionA").Bind(moduleSettings);
                #endregion

                #region 初始化数据

                var writeDataLen = moduleSettings.WriteDataSize / 2;

                var arr = new char[writeDataLen];
                for (var i = 0; i < writeDataLen; i++)
                {
                    arr[i] = 'a';
                }

                var readDataLen = moduleSettings.ReadDataSize / 2;
                #endregion

                #region 创建文件及其内存映射文件
                var capity = 1024 * 1024 * 1024 * moduleSettings.FileCapity;//文件容量
                var fs = new FileStream(moduleSettings.FileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
                var memoryFile = MemoryMappedFile.CreateFromFile(fs, moduleSettings.MapName, capity,
                   MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);//单个内存映射文件

                #endregion

                #region 并行计算

                var writeOffsets = new List<long>();
                if (moduleSettings.IsParallel)
                {
                    for (var i = 0; i < moduleSettings.WriteThreadCount; i++)
                    {
                        var offset = i * writeDataLen * 2;
                        writeOffsets.Add(offset);

                    }
                }

                var readOffsets = new List<long>();
                if (moduleSettings.IsParallel)
                {
                    for (var i = 0; i < moduleSettings.ReadThreadCount; i++)
                    {
                        var offset = i * writeDataLen * 2;
                        readOffsets.Add(offset);

                    }
                }

                #endregion

                #region 写数据任务

                //开启线程写入数据
                //Task.Factory.StartNew(() =>
                var write = new Thread(() =>
                {
                    if (!moduleSettings.IsWrite) return;
                    XTrace.Log.Info($"内存映射文件容量：{moduleSettings.FileCapity}G,当前循环次数：{moduleSettings.WriteThreadCount:n0},单次写入的数据大小：{moduleSettings.WriteDataSize:n0}B");
                    XTrace.Log.Info("write task start...");
                    var writeThreadCount = moduleSettings.WriteThreadCount;
                    // var threads = new Thread[writeThreadCount];
                    var accessor = memoryFile.CreateViewAccessor(0, writeThreadCount * writeDataLen * 2, MemoryMappedFileAccess.ReadWrite);
                    var sw = new Stopwatch();
                    sw.Start();

                    if (!moduleSettings.IsParallel)
                    {
                        for (var i = 0; i < writeThreadCount; i++)
                        {
                            var offset = i * writeDataLen * 2;
                            //Write(0, capity, arr, memoryFile);
                            Write(accessor, offset, arr, memoryFile);
                        }
                    }

                    #region 并行计算
                    //for (var i = 0; i < writeThreadCount; i++)
                    //{
                    //    var offset = i * dataLen * 2;
                    //    threads[i] = new Thread(() =>
                    //    {
                    //        //Write(0, capity, arr, memoryFile);
                    //        Write(offset, arr, memoryFile);
                    //    })
                    //    { IsBackground = true };
                    //    threads[i].Start();
                    //    //threads[i].Join();
                    //}
                    if (moduleSettings.IsParallel)
                    {
                        Parallel.ForEach(writeOffsets, offset =>
                        {
                            Write(accessor, offset, arr, memoryFile);
                        });
                    }

                    #endregion

                    sw.Stop();

                    XTrace.Log.Info("write task finished... ");
                    var cost = sw.ElapsedMilliseconds;
                    var writeData = writeThreadCount * moduleSettings.WriteDataSize / 1024 / 1024;//MB
                    if ((cost / 1000) == 0)
                        XTrace.Log.Error("写入耗时小于1S，建议调整参数，耗时调整到20S~60S之间！");
                    float seconds = (cost / 1000) == 0 ? 1 : (cost / 1000);//s
                    var speed = writeData / seconds;
                    var qps = writeThreadCount / seconds;
                    XTrace.Log.Info($"写入{writeData }MB的数据'a'性能参数如下: -QPS:{qps:n0}次/s，耗时:{cost:n0}ms，速度：{speed}MB/s,CPU耗时：{ (Int32)p.TotalProcessorTime.TotalSeconds}s，" +
                                    $"内存占用：{(Int32)(p.WorkingSet64 / 1024 / 1024)}MB,打开句柄数：{p.HandleCount}");
                });
                write.Start();

                #endregion

                #region 读数据任务

                //开启线程读取数据
                var read = new Thread(() =>
                {
                    if (!moduleSettings.IsRead) return;
                    XTrace.Log.Info("read task start...");
                    XTrace.Log.Info($"内存映射文件容量：{moduleSettings.FileCapity}G,当前循环次数：{moduleSettings.ReadThreadCount:n0},单次读取的数据大小：{moduleSettings.ReadDataSize:n0}B");
                    var readThreadCount = moduleSettings.ReadThreadCount;
                    //var tasks = new Task[readThreadCount];
                    var accessor = memoryFile.CreateViewAccessor(0, readThreadCount * readDataLen * 2, MemoryMappedFileAccess.ReadWrite);
                    var sw = new Stopwatch();
                    sw.Start();
                    long count = 0;
                    if (!moduleSettings.IsParallel)
                    {
                        for (var i = 0; i < readThreadCount; i++)
                        {
                            var offset = i * readDataLen * 2;
                            //tasks[i]=Task.Factory.StartNew(() =>
                            //{
                            var results = Read(accessor, offset, readDataLen, memoryFile); //Read(0, capity, dataLen, memoryFile);
                            if (results != readDataLen)
                                count++;
                            //});
                        }
                    }
                    //Task.WaitAll(tasks);

                    #region 并行计算

                    if (moduleSettings.IsParallel)
                    {
                        Parallel.ForEach(readOffsets, offset =>
                        {
                            var results = Read(accessor, offset, readDataLen, memoryFile);
                            if (results != readDataLen)
                                count++;
                        });
                    }

                    #endregion

                    sw.Stop();

                    XTrace.Log.Info("read task finished... ");
                    var cost = sw.ElapsedMilliseconds;
                    var readData = readThreadCount * moduleSettings.ReadDataSize / 1024 / 1024; //MB
                    if ((cost / 1000) == 0)
                        XTrace.Log.Error("读取耗时小于1S，建议调整参数，耗时调整到20S~60S之间！");
                    float seconds = (cost / 1000) == 0 ? 1 : (cost / 1000); //s
                    var speed = readData / seconds;
                    var qps = readThreadCount / seconds;
                    XTrace.Log.Info(
                        $"读取{readData}MB的数据'a'性能参数如下: -QPS:{qps:n0}次/s，耗时:{cost:n0}ms，速度：{speed}MB/s,CPU耗时：{(Int32)p.TotalProcessorTime.TotalSeconds}s，" +
                        $"内存占用：{(Int32)(p.WorkingSet64 / 1024 / 1024)}MB,打开句柄数：{p.HandleCount}");
                    XTrace.Log.Error($"读取失败数据计数：{count}");
                });
                read.Start();
                #endregion

            }
            catch (Exception e)
            {
                XTrace.Log.Error(e.Message);
                GC.Collect();
            }

            #endregion

            Console.ReadKey();
        }

        #region 私有方法

        /// <summary>
        ///   创建内存映射文件访问对象,内存映射视图对象accessor只提取了内存映射文件开头size个字节的内容
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="data"></param>
        /// <param name="memoryFile"></param>
        private static void Write(UnmanagedMemoryAccessor accessor, long offset, char[] data, MemoryMappedFile memoryFile)
        {
            var size = data.Length * 2;//bytes字节
            //var accessor = memoryFile.CreateViewAccessor(offset, size, MemoryMappedFileAccess.ReadWrite);
            accessor.WriteArray(offset, data, 0, data.Length);
        }

        /// <summary>
        /// 读取
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <param name="memoryFile"></param>
        /// <returns></returns>
        private static int Read(UnmanagedMemoryAccessor accessor, long offset, long len, MemoryMappedFile memoryFile)
        {
            var size = len * 2;//bytes字节
            //var accessor = memoryFile.CreateViewAccessor(offset, size, MemoryMappedFileAccess.ReadWrite);
            //读取字符长度  
            var arr = new char[len];
            //读取字符  
            return accessor.ReadArray(offset, arr, 0, Convert.ToInt32(len));
            //Console.Clear();
            // Console.Write(arr);
            //return arr;
        }
        /// <summary>
        ///      创建内存映射文件访问对象,内存映射视图对象accessor只提取了内存映射文件开头size个字节的内容
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="data"></param>
        private static void Init(long offset, long size, char data, MemoryMappedFile memoryFile)
        {

            var accessor = memoryFile.CreateViewAccessor(offset, size, MemoryMappedFileAccess.ReadWrite);

            //向其中写入了size/2个“e”字符
            for (var i = 0; i < size / 2; i += 2)
            {
                accessor.Write(i, data);
            }

        }


        #endregion



    }
}
