using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using NewLife.Caching;
using NewLife.Log;
using NewLife.NoDb;
using NewLife.NoDb.Collections;
using NewLife.NoDb.IO;
using NewLife.NoDb.Storage;
using NewLife.Reflection;
using NewLife.Security;
using NewLife.Configuration;
using NewLife.Xml;

namespace Test
{
    /// <summary>
    /// test
    /// </summary>
    class Program
    {
        static void Main(String[] args)
        {
            XTrace.UseConsole();

            if (Debugger.IsAttached)
                Test2();
            else
            {
                try
                {
                    Test2();
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }

            Console.WriteLine("OK!");
            Console.ReadKey(true);
        }

        static void Test1()
        {
            var cfg = CacheConfig.Current;
            var set = cfg.GetOrAdd("nodb");
            if (set.Provider.IsNullOrEmpty())
            {
                set.Provider = "NoDb";
                set.Value = "no.db";

                cfg.Save();
            }

            var ch = Cache.Create(set);

            var str = ch.Get<String>("name");
            Console.WriteLine(str);

            ch.Set("name", "大石头 {0}".F(DateTime.Now));

            str = ch.Get<String>("name");
            Console.WriteLine(str);

            ch.Bench();
        }

        static void Test2()
        {
            //Console.ReadKey();

            // GC闭嘴
            //GC.TryStartNoGCRegion(10_000_000);

            var count = 10_000_000L;
            var sw = Stopwatch.StartNew();
            var ms = 0L;

            using (var mmf = new MemoryFile("heap.db") { Log = XTrace.Log })
            using (var hp = new Heap(mmf, 256, 370L))
            {
                var list = new Block[count];
                for (var i = 0; i < count; i++)
                {
                    // 申请随机大小
                    list[i] = hp.Alloc(15);
                    //list.Add(bk);
                }
                sw.Stop();
                // 结果
                foreach (var pi in hp.GetType().GetProperties())
                {
                    Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
                }

                ms = sw.ElapsedMilliseconds;
                Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", ms, count * 1000L / ms);

                sw.Reset(); sw.Restart();
                for (var i = 0; i < count; i++)
                {
                    hp.Free(list[i]);
                }
                ms = sw.ElapsedMilliseconds;
                // 结果
                foreach (var pi in hp.GetType().GetProperties())
                {
                    Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
                }
                Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", ms, count * 1000L / ms);
            }
            XTrace.Log.Info("耗时：{0:n0}ms 整体速度 {1:n0}ops", ms, count * 1000L / ms);
        }

        static void Test3()
        {
            using (var mmf = new MemoryFile("list.db"))
            {
                var list = new MemoryList<Block>(mmf, 16, 1600, false);
                for (var i = 0; i < 7; i++)
                {
                    list.Add(new Block(i * 16, 998));
                }
                Console.WriteLine(list.Count);
                list.Insert(2, new Block(333, 444));
                var idx = list.IndexOf(new Block(32, 998));
                Console.WriteLine(idx);
                list.RemoveAt(5);
                foreach (var item in list)
                {
                    Console.WriteLine(item);
                }
                var arr = list.ToArray();
                //list.Dispose();
            }
        }

        static void Test4()
        {
            //var count = Config.GetConfig("readcount", 10_000_000L);
            // var size = Config.GetConfig("size", 16);
            var count = 80_000_000L;
            var sw = Stopwatch.StartNew();
            var ms = 0L;
            using (var mmf = new MemoryFile("queue.db") { Log = XTrace.Log })
            using (var qu = new MemoryQueue<Block>(mmf, 16, 16 * 1024 * 1024 * 1024L, false))
            {
                // XTrace.Log.Info("文件大小：{0:n0}GB", size);
                XTrace.Log.Info("队列总数：{0:n0}", qu.Count);
                XTrace.Log.Info("准备插入：{0:n0}", count);
                qu.View.GetView(0, (qu.Count + count) * 16);
                for (var i = 0L; i < count; i++)
                {
                    qu.Enqueue(new Block(i * 16, 998));
                }
                XTrace.Log.Info("队列总数：{0:n0}", qu.Count);
                Console.WriteLine(qu.Peek());

                ms = sw.ElapsedMilliseconds;
                XTrace.Log.Info("写入速度 {0:n0}ops", count * 1000L / ms);
            }
            XTrace.Log.Info("耗时：{0:n0}ms 整体速度 {1:n0}ops", ms, count * 1000L / ms);
        }
    }
}