/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace s3tests2
{
	[TestClass]
	public class Inventory : TestBase
	{
		
		[TestMethod("test_list_bucket_inventory")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "List")]
		[TestProperty(MainData.Explanation, "버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_list_bucket_inventory()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();

			var Response = Client.ListBucketInventoryConfigurations(BucketName);
			Assert.AreEqual(0, Response.InventoryConfigurationList);
		}

		[TestMethod("test_put_bucket_inventory")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Put")]
		[TestProperty(MainData.Explanation, "버킷에 인벤토리를 설정할 수 있는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_bucket_inventory()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{BucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);
		}

		[TestMethod("test_check_bucket_inventory")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "버킷에 인벤토리가 설정되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_check_bucket_inventory()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{BucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);

			var Response = Client.ListBucketInventoryConfigurations(BucketName);
			Assert.Single(Response.InventoryConfigurationList);
		}

		[TestMethod("test_get_bucket_inventory")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "버킷에 설정된 인벤토리 설정을 가져올 수 있는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_bucket_inventory()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{BucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);

			var Response = Client.GetBucketInventoryConfiguration(BucketName, InventoryId);
			Assert.AreEqual(InventoryId, Response.InventoryConfiguration.InventoryId);
		}

		[TestMethod("test_delete_bucket_inventory")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Delete")]
		[TestProperty(MainData.Explanation, "버킷에 설정된 인벤토리 설정을 삭제할 수 있는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_delete_bucket_inventory()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";


			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{BucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);

			Client.DeleteBucketInventoryConfiguration(BucketName, InventoryId);

			var Response = Client.ListBucketInventoryConfigurations(BucketName);
			Assert.Empty(Response.InventoryConfigurationList);
		}

		[TestMethod("test_get_bucket_inventory_not_exist")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_get_bucket_inventory_not_exist()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var Response = Client.GetBucketInventoryConfiguration(BucketName, InventoryId);
			Assert.AreEqual(HttpStatusCode.NotFound, Response.HttpStatusCode);
		}

		[TestMethod("test_delete_bucket_inventory_not_exist")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_delete_bucket_inventory_not_exist()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(() => Client.DeleteBucketInventoryConfiguration(BucketName, InventoryId));
			Assert.AreEqual(HttpStatusCode.NotFound, GetStatus(e));
			Assert.AreEqual(MainData.NoSuchConfiguration, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_invalid_bucket_name")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "존재하지 않는 버킷에 인벤토리 설정을 추가하려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_put_bucket_inventory_not_exist()
		{
			var Client = GetClient();
			var BucketName = GetNewBucketName();
			var TargetBucketName = GetNewBucketName();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.NotFound, GetStatus(e));
			Assert.AreEqual(MainData.NoSuchBucket, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_id_not_exist")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인")]
		public void test_put_bucket_inventory_id_not_exist()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName();

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = "",
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.AreEqual(MainData.InvalidConfigurationId, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_target_not_exist", Skip = "S3에서는 타깃 버킷이 존재하지 않아도 인벤토리 설정을 추가할 수 있음")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "타깃 버킷이 존재하지 않을 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_put_bucket_inventory_target_not_exist()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.NotFound, GetStatus(e));
			Assert.AreEqual(MainData.NoSuchBucket, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_invalid_format")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_put_bucket_inventory_invalid_format()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = "JSON",
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.AreEqual(MainData.MalformedXML, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_invalid_frequency")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_put_bucket_inventory_invalid_frequency()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = "Hourly" },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.AreEqual(MainData.MalformedXML, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_invalid_case", Skip = "C# S3에서는 대소문자를 자동 치환")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_put_bucket_inventory_invalid_case()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = "CURRENT",
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = "DAILY" },
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.AreEqual(MainData.MalformedXML, GetErrorCode(e));
		}

		[TestMethod("test_put_bucket_inventory_prefix")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Put")]
		[TestProperty(MainData.Explanation, "접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인")]
		public void test_put_bucket_inventory_prefix()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";
			var Prefix = "my-prefix";

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
						Prefix = Prefix,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);

			var Response = Client.GetBucketInventoryConfiguration(BucketName, InventoryId);
			Assert.AreEqual(InventoryId, Response.InventoryConfiguration.InventoryId);
			Assert.AreEqual(Prefix, Response.InventoryConfiguration.Destination.S3BucketDestination.Prefix);
		}

		[TestMethod("test_put_bucket_inventory_optional")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Put")]
		[TestProperty(MainData.Explanation, "옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인")]
		public void test_put_bucket_inventory_optional()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";
			var InventoryOptionalFields = new List<InventoryOptionalField> { InventoryOptionalField.Size, InventoryOptionalField.LastModifiedDate };

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
				InventoryOptionalFields = InventoryOptionalFields,
			};

			Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration);

			var Response = Client.GetBucketInventoryConfiguration(BucketName, InventoryId);
			Assert.AreEqual(InventoryId, Response.InventoryConfiguration.InventoryId);
			Assert.AreEqual(InventoryOptionalFields, Response.InventoryConfiguration.InventoryOptionalFields);
		}

		[TestMethod("test_put_bucket_inventory_invalid_optional")]
		[TestProperty(MainData.Major, "Inventory")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "옵션을 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		public void test_put_bucket_inventory_invalid_optional()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var InventoryId = "my-inventory";
			var InventoryOptionalFields = new List<InventoryOptionalField> { "SIZE", "LAST_MODIFIED_DATE" };

			var InventoryConfiguration = new InventoryConfiguration
			{
				Destination = new InventoryDestination
				{
					S3BucketDestination = new InventoryS3BucketDestination
					{
						BucketName = $"arn:aws:s3:::{TargetBucketName}",
						InventoryFormat = InventoryFormat.CSV,
					}
				},
				InventoryId = InventoryId,
				IncludedObjectVersions = InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = InventoryFrequency.Daily },
				InventoryOptionalFields = InventoryOptionalFields,
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketInventoryConfiguration(BucketName, InventoryConfiguration));
			Assert.AreEqual(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.AreEqual(MainData.MalformedXML, GetErrorCode(e));
		}
	}

}