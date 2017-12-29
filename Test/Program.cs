using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using NewLife.Caching;
using NewLife.Log;
using NewLife.NoDb;
using NewLife.NoDb.Collections;
using NewLife.NoDb.Storage;
using NewLife.Reflection;
using NewLife.Security;

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
                Test3();
            else
            {
                try
                {
                    Test3();
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

            var bk = new Block(256, 20_000_000_000);
            var hp = new Heap(bk);

            var count = 10_000_000;
            //var list = new List<Block>(count);
            var list = new Block[count];
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < count; i++)
            {
                // 申请随机大小
                list[i] = hp.Alloc(1600);
                //list.Add(bk);
            }
            sw.Stop();
            // 结果
            foreach (var pi in hp.GetType().GetProperties())
            {
                Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
            }
            Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", sw.ElapsedMilliseconds, count * 1000L / sw.ElapsedMilliseconds);

            sw.Reset(); sw.Restart();
            for (var i = 0; i < count; i++)
            {
                hp.Free(list[i]);
            }
            sw.Stop();
            // 结果
            foreach (var pi in hp.GetType().GetProperties())
            {
                Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
            }
            Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", sw.ElapsedMilliseconds, count * 1000L / sw.ElapsedMilliseconds);

            //var db = new Database("test.db");
            //Console.WriteLine(db.Version);

        }

        static void Test3()
        {
            using (var mmf = MemoryMappedFile.CreateFromFile("list.db".GetFullPath(), FileMode.OpenOrCreate, "list", 16 * 1024))
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
                //list.Dispose();
            }
        }
    }
}