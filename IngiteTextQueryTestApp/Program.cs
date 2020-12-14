using ConstructionSearchApi.Ignite;
using System;

namespace IngiteTextQueryTestApp
{
   public class Program
    {
        public static IgniteThinClientService _igniteThinClientService;
        private static string STUDENT_CACHE_NAME = "STUDENT";
        private static string CONSTRUCTIONCOMMON_CACHE_NAME = "F2DEDF6E-393E-42BC-9BB3-E835A1063B30_6EFB69B0-269F-4F92-98CF-24BC0D34BA98_COMMON";


        static void Main(string[] args)
        {
            _igniteThinClientService = new IgniteThinClientService("localhost");
            Program program = new Program();
            program.TestTextQueries();
            Console.ReadKey();
        }

        private async void TestTextQueries()
        {
            var result = await _igniteThinClientService.SearchTextQueryonApp(CONSTRUCTIONCOMMON_CACHE_NAME, "");

            //var result = await _igniteThinClientService.SearchRecordAsync(STUDENT_CACHE_NAME, "");
        }
    }
}
