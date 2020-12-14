using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Eviction;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Communication.Tcp;
using Apache.Ignite.Core.Configuration;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using ConstructionModels;
using ConstructionModels.Construction;
using DotNetIgniteClientServerApp.CacheStoreFactory;
using DotNetIgniteClientServerApp.Interface;
using DotNetIgniteClientServerApp.Models;
using IgnitePersistenceApp.ConstructionApp.CacheStoreFactory;
using IgnitePersistenceApp.ConstructionApp.Common;
using IgnitePersistenceApp.Logger;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace ConstructionSearchApi.Ignite
{
    public class IgniteThickClientTextQueryService
    {
        private readonly IgniteClientConfiguration _igniteClientConfiguration;
        private readonly string _endPoint;
        private readonly string[] _endPoints;

        public IgniteThickClientTextQueryService(string endPoint)
        {
            var endpoints = new List<string>();
            if (endPoint.Contains(","))
            {
                endpoints = endPoint.Split(',').ToList();
            }
            else
            {
                endpoints.Add(endPoint);
            }
            _igniteClientConfiguration = new IgniteClientConfiguration
            {
                Endpoints = endpoints,
                SocketTimeout = TimeSpan.FromSeconds(20000)
            };
            this._endPoint = endPoint;
            this._endPoints = endpoints.ToArray();
        }

        public async Task<string> SearchTextQueryonApp(string cacheName,string searchText)
        {
            try
            {
                var repeat = "";
                do
                {
                    var cache = await GetIConstructionCacheStore(cacheName);
                    var textquery = new TextQuery(typeof(CommonConstruction), "Metric");
                    var cursor = cache.Query(textquery).GetAll();
                    foreach (var cacheEntry in cursor)
                    {
                        Console.WriteLine(cacheEntry.Value);
                    }
                    Console.WriteLine("do you want to repeat(y/n)");
                    repeat = Console.ReadLine();
                } while (repeat == "y");


                //var sqlquery = new SqlFieldsQuery("select uuid from CommonConstruction");
                //var cursor = cache.Query(sqlquery).GetAll();
                Console.WriteLine("End of query");

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            string res = "";
            return await Task.FromResult(res);
        }

       
        private async Task<ICache<string, IConstructionCacheStore>> GetIConstructionCacheStore(string cacheName)
        {
            try
            {
                var client = GetOrStartIgniteClient();
                var cache = client.GetOrCreateCache<string, IConstructionCacheStore>(cacheName);
                return cache;
            }
            catch (CacheException ex)
            {
                //Per cache Per Node one time manual exception
                var connectionString = "";
                var cacheConfig = await CreateIConstructionCacheStoreCache(cacheName, "Construction_Common", connectionString);
                _IGNITE_CLIENT.AddCacheConfiguration(cacheConfig);
                return _IGNITE_CLIENT.GetOrCreateCache<string, IConstructionCacheStore>(cacheName);
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public async Task<CacheConfiguration> CreateIConstructionCacheStoreCache(string cacheName, string capabilityName, string connectionString)
        {
            ConstructionQueryEntities _queryEntities = new ConstructionQueryEntities();
             EventLogger _logger = new EventLogger();


            var queryList = await _queryEntities.CreateQueryEntitiesListAsync(capabilityName);
            cacheName = cacheName.ToUpper();
            var templateName = cacheName.Remove(cacheName.Length - 1, 1) + "*";
            var cacheCfg = new CacheConfiguration(templateName)
            {
                Name = cacheName,
                CacheStoreFactory = new ConstructionTenantCacheStoreFactory(_logger, connectionString),
                KeepBinaryInStore = false,  // Cache store works with deserialized data.
                ReadThrough = true,
                WriteThrough = true,
                WriteBehindEnabled = true,
                QueryEntities = queryList,
                WriteBehindFlushThreadCount = 2,
                CacheMode = CacheMode.Partitioned,
                Backups = 0,
                DataRegionName = "IgniteDataRegion",
                EvictionPolicy = new LruEvictionPolicy
                {
                    MaxSize = 100000
                },
                GroupName= "F2DEDF6E-393E-42BC-9BB3-E835A1063B30_6EFB69B0-269F-4F92-98CF-24BC0D34BA98"
            };

            return cacheCfg;
        }





















        public async Task<string> SearchRecordAsync(string cacheName, string searchText)
        {
            try
            {
                var cache = await GetICustomStoreCache(cacheName, "");
                var textquery = new TextQuery("Student", "Good");
                var cursor = cache.Query(textquery).GetAll();
                foreach (var cacheEntry in cursor)
                {
                    Console.WriteLine(cacheEntry.Value);
                }
                Console.WriteLine("End of query");

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            string res = "";
            return await Task.FromResult(res);
        }

        private async Task<ICache<string, ICustomCacheStore>> GetICustomStoreCache(string cacheName, string message)
        {

            try
            {
                var client = GetOrStartIgniteClient();
                var cache = client.GetOrCreateCache<string, ICustomCacheStore>(cacheName);
                return cache;
            }
            catch (CacheException ex)
            {
               
                //Per cache Per Node one time manual exception

                var connectionString = "";
                var cacheConfig = await CreateICustomCacheStoreCache(cacheName, "Construction", connectionString);
                _IGNITE_CLIENT.AddCacheConfiguration(cacheConfig);
                return _IGNITE_CLIENT.GetOrCreateCache<string, ICustomCacheStore>(cacheName);
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public async Task<CacheConfiguration> CreateICustomCacheStoreCache(string cacheName, string capabilityName, string connectionString)
        {

            QueryEntityIndexFields[] queryEntityIndexFieldsList = new[] {            //Create For People
            new QueryEntityIndexFields(){PrimaryKeys=new [] { "EntityId" }, ModelType=  typeof(Student) }
            
            };

            var queryEntity = new QueryEntity(typeof(string), typeof(Student));

            List<QueryEntity> queryList = new List<QueryEntity>();
            foreach (var modelObject in queryEntityIndexFieldsList)
            {
                var query = new QueryEntity(typeof(string), modelObject.ModelType)
                {
                    Indexes = new List<QueryIndex>(modelObject.PrimaryKeys.Count() + 1)
                        {
                            new QueryIndex(true,0,modelObject.PrimaryKeys)
                        }
                };
                queryList.Add(query);
            }
            
            cacheName = cacheName.ToUpper();
            var templateName = cacheName.Remove(cacheName.Length - 1, 1) + "*";
            var cacheCfg = new CacheConfiguration(templateName)
            {
                Name = cacheName,
                CacheStoreFactory = new DataCacheStoreFactory(),
                KeepBinaryInStore = false,  // Cache store works with deserialized data.
                ReadThrough = true,
                WriteThrough = true,
                WriteBehindEnabled = true,
                QueryEntities = queryList,
                WriteBehindFlushThreadCount = 2,
                CacheMode = CacheMode.Partitioned,
                Backups = 0,
                DataRegionName = "IgniteDataRegion",
                EvictionPolicy = new LruEvictionPolicy
                {
                    MaxSize = 100000
                }
            };



            return cacheCfg;
        }

        public IIgnite StartIgniteClientOnServiceNode(string nodeName)
        {
           // this._NodeName = nodeName;
            return StartIgniteClient();
        }
        public IIgnite GetOrStartIgniteClient()
        {
            return StartIgniteClient();
        }
        public IgniteConfiguration GetIgniteConfiguration()
        {
            IgniteConfiguration config = null;
            try
            {
                config = new IgniteConfiguration
                {

                    IgniteInstanceName = "TextQueryTestNode",

                    DiscoverySpi = new TcpDiscoverySpi
                    {
                        IpFinder = new TcpDiscoveryStaticIpFinder
                        {
                            Endpoints = _endPoints//new[] { "104.211.24.238" }//127.0.0.1 or ip
                        },
                        SocketTimeout = TimeSpan.FromSeconds(3000)
                    },
                    DataStorageConfiguration = new DataStorageConfiguration()
                    {
                        DefaultDataRegionConfiguration = new DataRegionConfiguration()
                        {
                            Name = "IgniteDataRegion",
                            PersistenceEnabled = true
                        },
                        StoragePath = "C:\\client\\storage",
                        WalPath = "C:\\client\\wal",
                        WalArchivePath = "C:\\client\\walArchive"
                    },
                    WorkDirectory = "C:\\client\\work",
                    // Explicitly configure TCP communication SPI by changing local port number for
                    // the nodes from the first cluster.
                    CommunicationSpi = new TcpCommunicationSpi()
                    {
                        LocalPort = 47100
                    },
                    PeerAssemblyLoadingMode = Apache.Ignite.Core.Deployment.PeerAssemblyLoadingMode.CurrentAppDomain

                };
            }
            catch (Exception ex)
            {
                throw;
            }
            return config;
        }

        IIgniteClient THIN_IGNITE_CLIENT = null;
        public static IIgnite _IGNITE_CLIENT;

        public IIgnite StartIgniteClient()
        {
            try
            {
                if (_IGNITE_CLIENT == null)
                {
                    Ignition.ClientMode = true;
                    // Connect to the cluster.
                    // _IGNITE_CLIENT = Ignition.Start("C:\\Users\bizruntime-86\\source\\client-config.xml");

                    _IGNITE_CLIENT = Ignition.TryGetIgnite("TextQueryTestNode");
                    if (_IGNITE_CLIENT == null)
                        _IGNITE_CLIENT = Ignition.Start(GetIgniteConfiguration());
                    //_IGNITE_CLIENT = Ignition.Start(Directory.GetCurrentDirectory()+ "\\client-config.xml");//GetIgniteConfiguration());
                }

            }
            catch (Exception ex)
            {
                throw;
            }

            return _IGNITE_CLIENT;
        }

    }

    //public class Person: IConstructionCacheStore
    //{
    //    [QueryTextField]
    //    public string Payload { get; set; }
    //}

    struct QueryEntityIndexFields
    {
        public string[] PrimaryKeys;
        public Type ModelType;
    }
}
