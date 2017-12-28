using System;
using System.Collections.Generic;
using System.Text;

namespace test1
{
    public class MySettings
    {
        public string ApplicationName { get; set; }
    }

    public class MyModuleSettings
    {
        /// <summary>
        /// 是否开启写任务
        /// </summary>
        public bool IsWrite { get; set; }

        /// <summary>
        /// 是否开启读任务
        /// </summary>
        public bool IsRead { get; set; }
        /// <summary>
        /// 写入线程数
        /// </summary>
        public long WriteThreadCount { get; set; }
        /// <summary>
        /// 写入数据大小
        /// </summary>
        public long WriteDataSize { get; set; }
        /// <summary>
        /// 读取线程数
        /// </summary>
        public long ReadThreadCount { get; set; }
        /// <summary>
        /// 读取数据大小
        /// </summary>
        public long ReadDataSize { get; set; }
        /// <summary>
        /// 文件容量
        /// </summary>
        public long FileCapity { get; set; }
        /// <summary>
        /// 文件名称及路径
        /// </summary>
        public string FileName { get; set; }
        /// <summary>
        /// 内存映射文件名称
        /// </summary>
        public string MapName { get; set; }

        /// <summary>
        /// 是否开启并行计算
        /// </summary>
        public bool IsParallel { get; set; }

    }
}
