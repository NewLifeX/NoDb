using System;
using System.Diagnostics;
using System.Threading.Tasks;
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
                Test5();
            else
            {
                try
                {
                    Test5();
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

        static void TestArray()
        {
            var count = 10_000_000L;
            using (var mmf = new MemoryFile("list.db"))
            {
                var sw = Stopwatch.StartNew();

                var arr = new MemoryArray<Int64>(mmf, count);
                for (var i = 0; i < count; i++)
                {
                    arr[i] = i;
                }

                sw.Stop();
                var ms = sw.ElapsedMilliseconds;
                Console.WriteLine("赋值[{0:n0}] {1:n0}tps", count, count * 1000L / ms);

                sw.Restart();
                for (var i = 0; i < count; i++)
                {
                    var n = arr[i];
                }
                sw.Stop();
                ms = sw.ElapsedMilliseconds;
                Console.WriteLine("取值[{0:n0}] {1:n0}tps", count, count * 1000L / ms);
            }
        }

        static void TestQueue()
        {
            var count = 100_000_000L;
            using (var mmf = new MemoryFile("queue.db") { Log = XTrace.Log })
            using (var qu = new NewLife.NoDb.Collections.MemoryQueue<Block>(mmf, 16, 16 * 1024 * 1024 * 1024L, false))
            {
                XTrace.Log.Info("队列总数：{0:n0}", qu.Count);
                XTrace.Log.Info("准备插入：{0:n0}", count);
                qu.View.GetView(0, (qu.Count + count) * 16);

                var sw = Stopwatch.StartNew();
                for (var i = 0L; i < count; i++)
                {
                    qu.Enqueue(new Block(i * 16, 998));
                }
                //Parallel.For(0, count, i =>
                //{
                //    qu.Enqueue(new Block(i * 16, 998));
                //});
                sw.Stop();
                var ms = sw.ElapsedMilliseconds;
                Console.WriteLine("入队[{0:n0}] {1:n0}tps", count, count * 1000L / ms);
                XTrace.Log.Info("队列总数：{0:n0}", qu.Count);

                sw.Restart();
                //Console.WriteLine(qu.Peek());
                for (var i = 0L; i < count; i++)
                {
                    qu.Dequeue();
                }
                sw.Stop();
                ms = sw.ElapsedMilliseconds;
                Console.WriteLine("出队[{0:n0}] {1:n0}tps", count, count * 1000L / ms);
                XTrace.Log.Info("队列总数：{0:n0}", qu.Count);
            }
        }

        static void Test5()
        {
            var count = 24 * 3600L;
            count *= 100;
            //count = 13;
            var buf = "01234567890ABCD".GetBytes();
            using (var db = new ListDb("List.db", false, false))
            {
                db.Log = XTrace.Log;
                db.Init();

                var sw = Stopwatch.StartNew();
                //count = 1;
                for (var i = 0; i < count; i++)
                {
                    //var bk = db[i];
                    //Console.WriteLine(bk);
                    //db.Set(i, buf);
                    //db.Set(i, Rand.NextString(15).GetBytes());
                    db.Add(buf);
                }
                sw.Stop();
                var ms = sw.Elapsed.TotalMilliseconds;
                XTrace.WriteLine("写入{0:n0}，耗时{1:n0}ms，速度 {2:n0}tps", count, ms, count * 1000 / ms);
            }

            count *= 10;
            using (var db = new ListDb("List.db", true))
            {
                var total = db.Count;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < count; i++)
                {
                    buf = db.Get(i % total);
                }
                sw.Stop();
                var ms = sw.Elapsed.TotalMilliseconds;
                XTrace.WriteLine("读取{0:n0}，耗时{1:n0}ms，速度 {2:n0}rps", count, ms, count * 1000 / ms);
            }
        }
    }
}