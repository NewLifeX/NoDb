using System;
using System.Collections.Generic;
using System.Diagnostics;
using NewLife.Caching;
using NewLife.Log;
using NewLife.NoDb;
using NewLife.NoDb.Storage;
using NewLife.Reflection;
using NewLife.Security;

namespace Test
{
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
            GC.TryStartNoGCRegion(10_000_000);

            var bk = new Block(256, 16_000_000_000);
            var hp = new Heap(bk);

            var count = 10_000_000;
            var list = new List<Block>(count);
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < count; i++)
            {
                // 申请随机大小
                bk = hp.Alloc(1601);
                list.Add(bk);
            }
            sw.Stop();
            // 结果
            foreach (var pi in hp.GetType().GetProperties())
            {
                Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
            }
            Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", sw.ElapsedMilliseconds, count * 1000L / sw.ElapsedMilliseconds);

            sw.Reset(); sw.Restart();
            foreach (var item in list)
            {
                hp.Free(item);
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
    }
}