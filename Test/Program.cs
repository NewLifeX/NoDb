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

            var bk = new Block(256, 16_000_000_000);
            var hp = new Heap(bk);

            var list = new List<Block>();
            var count = 1_000_000;
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < count; i++)
            {
                // 一般几率申请，一半释放
                if (list.Count == 0 || Rand.Next(2) == 0)
                {
                    // 申请随机大小
                    bk = hp.Alloc(Rand.Next(1, 10_000_000));
                    list.Add(bk);
                }
                else
                {
                    // 随机释放一块
                    var idx = Rand.Next(0, list.Count);
                    bk = list[idx];
                    list.RemoveAt(idx);
                    hp.Free(bk);
                }
            }
            sw.Stop();
            // 结果
            foreach (var pi in hp.GetType().GetProperties())
            {
                Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
            }
            Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", sw.ElapsedMilliseconds, count * 1000L / sw.ElapsedMilliseconds);

            Console.WriteLine();
            foreach (var item in list)
            {
                hp.Free(item);
            }
            foreach (var pi in hp.GetType().GetProperties())
            {
                Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
            }

            //var db = new Database("test.db");
            //Console.WriteLine(db.Version);

        }
    }
}