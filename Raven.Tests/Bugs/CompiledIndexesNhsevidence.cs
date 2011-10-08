using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using NLog;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Linq;
using Raven.Database.Util;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CompiledIndexesNhsevidence : RavenTest
	{
		[Fact]
		public void CanGetCorrectResults()
		{
			for (int x = 0; x < 50; x++)
			{
				Trace.WriteLine("Iteration #" + x);
				using (var store = CreateStore())
				{
					for (int i = 0; i < 12; i++)
					{
						AddRecord(store, 1);
					}
					ReadRecords(store, 600);
				}
			}
		}

		private static void ReadRecords(IDocumentStore store, int shouldBe)
		{
			using(var session = store.OpenSession())
			{
				for (int i = 0; i < 6; i++)
				{
					int count = session.Advanced.LuceneQuery<object>("view" + (i+1)).WaitForNonStaleResultsAsOfLastWrite().QueryResult.TotalResults;
					if(count != shouldBe)
					{
						var boundedMemoryTarget = LogManager.Configuration.AllTargets.OfType<BoundedMemoryTarget>().FirstOrDefault();
						var logEventInfos = boundedMemoryTarget.GeneralLog.ToArray();
						foreach (var logEventInfo in logEventInfos)
						{
							Console.WriteLine(logEventInfo);
						}
					}
					Assert.Equal(shouldBe, count);
				}
			}
		}

		private static void AddRecord(IDocumentStore store,int records)
		{
			using(var session = store.OpenSession())
			{
				for (int i = 0; i < records; i++)
				{
					var item = new TestClass {Items = new List<Item>()};
					for (int j = 0; j < 50; j++)
					{
						item.Items.Add(new Item()
						{
							Id = j + 1,
							Email = string.Format("rob{0}@text.com", i + 1).PadLeft(200, (char) i),
							Name = string.Format("rob{0}", i + 1).PadLeft(300, (char) i)
						});
					}
					session.Store(item);
				}
				session.SaveChanges();
			}
		}

		private static EmbeddableDocumentStore CreateStore()
		{
			var store = new EmbeddableDocumentStore
			{
				Conventions = Conventions.Document,
				Configuration =
				{
					RunInMemory = true,
					MaxNumberOfParallelIndexTasks = 1
				},

			};
			store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(
				typeof(View1),
				typeof(View2),
				typeof(View3),
				typeof(View4),
				typeof(View5),
				typeof(View6)
				));
			store.Initialize();
			return store;
		}

		public class Item
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
		}

		[JsonObject(IsReference = true)]
		public class TestClass
		{
			public string Id { get; set; }
			public List<Item> Items { get; set; }
		}

		public class TestClassView : AbstractViewGenerator
		{
			public TestClassView()
			{

				ForEntityNames.Add("TestClass");
				MapDefinitions.Add(MapToPaths);
				ReduceDefinition = Reduce;
				GroupByExtraction = doc => doc.UserId;

				AddField("UserId");
				AddField("Name");
				AddField("Email");

				Indexes.Add("UserId", FieldIndexing.NotAnalyzed);
			}


			private IEnumerable<dynamic> Reduce(IEnumerable<dynamic> source)
			{
				foreach (var o in source)
				{
					//Console.WriteLine("{0},{1}",o.__document_id, o.UserId);
					yield return new
					{
						o.__document_id,
						o.UserId,
						o.Name,
						o.Email
					};
				}
			}

			private IEnumerable<dynamic> MapToPaths(IEnumerable<dynamic> source)
			{
				foreach (var o in source)
				{
					if(o["@metadata"]["Raven-Entity-Name"] != "TestClasses")
						continue;
					var testClass = FromRaven(o);
					
					foreach (var item in testClass.Items)
					{
						yield return new
						{
							__document_id = o.Id,
							UserId = item.Id,
							item.Name,
							item.Email
						};
					}
				}
				yield break;
			}


			TestClass FromRaven(dynamic o)
			{
				var jobject = (RavenJObject)o.Inner;
				var item = ((TestClass)jobject.Deserialize(typeof(TestClass), Conventions.Document));

				if (item == null)
					throw new ApplicationException("Deserialisation failed");

				return item;
			}
		}



		public static class Conventions
		{
			public static readonly DocumentConvention Document = new DocumentConvention
			{
				FindTypeTagName = t => t.GetType() == typeof(TestClass) ? "testclass" : null,
				MaxNumberOfRequestsPerSession = 3000,
				DocumentKeyGenerator = doc =>
				{
					if (doc is TestClass)
						return ((TestClass)doc).Id;

					return null;
				}
			};
		}

		[DisplayName("view1")]
		public class View1 : TestClassView
		{
		}

		[DisplayName("view2")]
		public class View2 : TestClassView
		{
		
		}

		[DisplayName("view3")]
		public class View3 : TestClassView
		{
		}

		[DisplayName("view4")]
		public class View4 : TestClassView
		{
		}

		[DisplayName("view5")]
		public class View5 : TestClassView
		{
		}

		[DisplayName("view6")]
		public class View6 : TestClassView
		{
		}
	}
}