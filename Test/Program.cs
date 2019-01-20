using System;
using System.Diagnostics;
using NewLife.Log;
using NewLife.NoDb;
using NewLife.NoDb.Collections;
using NewLife.NoDb.IO;
using NewLife.NoDb.Storage;
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
            //var cfg = CacheConfig.Current;
            //var set = cfg.GetOrAdd("nodb");
            //if (set.Provider.IsNullOrEmpty())
            //{
            //    set.Provider = "NoDb";
            //    set.Value = "no.db";

            //    cfg.Save();
            //}

            //var ch = Cache.Create(set);

            //var str = ch.Get<String>("name");
            //Console.WriteLine(str);

            //ch.Set("name", "大石头 {0}".F(DateTime.Now));

            //str = ch.Get<String>("name");
            //Console.WriteLine(str);

            //ch.Bench();
        }

        static void Test2()
        {
            //Console.ReadKey();

            // GC闭嘴
            //GC.TryStartNoGCRegion(10_000_000);

            var count = 10_000_000L;
            var ms = 0L;
            var total = 0L;
#if DEBUG
            count = 10;
#endif

            using (var mmf = new MemoryFile("heap.db") { Log = XTrace.Log })
            using (var hp = new Heap(mmf, 256, 375_000_000, false))
            {
#if DEBUG
                hp.Log = XTrace.Log;
#endif
                hp.Init();
                //hp.Clear();

                Console.WriteLine("申请[{0:n0}]块随机大小（8~32字节）的内存", count);

                var sw = Stopwatch.StartNew();
                //count = 12;
                var list = new Block[count];
                for (var i = 0; i < count; i++)
                {
                    // 申请随机大小
                    var size = Rand.Next(8, 32);
                    list[i] = hp.Alloc(size);
                    //list.Add(bk);

#if DEBUG
                    Console.WriteLine("申请到 {0} Count={1} Used={2}", list[i], hp.Count, hp.Used);
#endif
                }
                sw.Stop();
                // 结果
                //foreach (var pi in hp.GetType().GetProperties())
                //{
                //    Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
                //}
                Console.WriteLine("{0} Count={1:n0} Used={2:n0}", hp, hp.Count, hp.Used);

                ms = sw.ElapsedMilliseconds;
                total += ms;
                Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", ms, count * 1000L / ms);

                Console.WriteLine("释放[{0:n0}]块内存", count);
                sw.Restart();
                hp.Free(list[4]);
                hp.Free(list[3]);
                hp.Free(list[5]);
                for (var i = 0; i < count; i++)
                {
                    if (i == 3 || i == 4 || i == 5) continue;

#if DEBUG
                    Console.WriteLine("释放 {0} Count={1} Used={2}", list[i], hp.Count, hp.Used);
#endif
                    hp.Free(list[i]);
                }
                sw.Stop();

                // 结果
                Console.WriteLine("{0} Count={1:n0} Used={2:n0}", hp, hp.Count, hp.Used);
                //foreach (var pi in hp.GetType().GetProperties())
                //{
                //    Console.WriteLine("{0}\t{1:n0}", pi.Name, hp.GetValue(pi));
                //}
                ms = sw.ElapsedMilliseconds;
                total += ms;
                Console.WriteLine("耗时：{0:n0}ms 速度 {1:n0}ops", ms, count * 1000L / ms);
            }
            XTrace.Log.Info("耗时：{0:n0}ms 整体速度 {1:n0}ops", total, count * 1000L / total);
        }

        static void Test3()
        {
            using (var mmf = new MemoryFile("list.db"))
            {
                var arr = new MemoryArray<Int64>(mmf, 1024 * 1024);
                for (var i = 0; i < 100; i++)
                {
                    var n = arr[i];
                    Console.WriteLine("{0}\t={1}", i, n);
                }

                //var list = new MemoryList<Block>(mmf, 16, 1600, false);
                //for (var i = 0; i < 7; i++)
                //{
                //    list.Add(new Block(i * 16, 998));
                //}
                //Console.WriteLine(list.Count);
                //list.Insert(2, new Block(333, 444));
                //var idx = list.IndexOf(new Block(32, 998));
                //Console.WriteLine(idx);
                //list.RemoveAt(5);
                //foreach (var item in list)
                //{
                //    Console.WriteLine(item);
                //}
                //var arr = list.ToArray();
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
            using (var qu = new NewLife.NoDb.Collections.MemoryQueue<Block>(mmf, 16, 16 * 1024 * 1024 * 1024L, false))
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

        static void Test5()
        {
            var count = 24 * 3600;
            count = 13;
            using (var db = new ListDb("List.db", count))
            {
                for (var i = 0; i < count; i++)
                {
                    db.Set(i, Rand.NextBytes(30));
                }
            }
        }
    }
}