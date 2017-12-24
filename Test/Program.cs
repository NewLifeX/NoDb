using System;
using System.Diagnostics;
using NewLife.Caching;
using NewLife.Log;
using NewLife.NoDb;

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

            var db = new Database("test.db");
            Console.WriteLine(db.Version);

        }
    }
}