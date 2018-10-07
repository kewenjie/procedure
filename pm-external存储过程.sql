/**
	修改记录
	2018/8/30  
		业务描述: 新建存储过程,将专区价格数据同步至TChannelSkuPriceSearch 
		修改人: 柯文杰
	2018/9/20
		业务描述: 同步数据时根据专区SKU表数据状态做判断,若状态为P 则删除 TChannelSkuPriceSearch 中对应的数据
		修改人: 柯文杰
**/


IF EXISTS (SELECT *  FROM  sys.procedures WHERE name='SyncDataToSearch')
DROP PROCEDURE SyncDataToSearch
GO

CREATE PROC SyncDataToSearch
AS
BEGIN
	DECLARE @lastSyncTime DATETIME
	DECLARE @changeDateField NVARCHAR(50)
	DECLARE @tableType NVARCHAR(50)
	DECLARE @tableName NVARCHAR(50),@channelIdField NVARCHAR(50),@channelId NVARCHAR(50),@salePriceType NVARCHAR(50)
	DECLARE @sql NVARCHAR(4000);
	--声明一个游标
	DECLARE rsdsCur CURSOR FOR 
	SELECT tableName,lastSyncTime,tableType,changeDateField,channelIdField,channelId,SalePriceType FROM rsds..TChannelSkuPriceSearchSyncConfig --where tableName='rsds..TABCPriceListItemSku';
	OPEN rsdsCur
	FETCH NEXT FROM rsdsCur INTO @tableName,@lastSyncTime,@tableType,@changeDateField,@channelIdField,@channelId,@salePriceType;
	WHILE (@@FETCH_STATUS <>-1)                        --游标读取下一个数据的状态，0表示读取成功  
		BEGIN
			print 'txl_start_01_  '+@tableType+' '+@tablename;
			--开始写核心代码
			IF(@tableType=1)
				BEGIN
					DECLARE @i INT,@ii INT,@num INT,@sql_count NVARCHAR(200),@count INT,@recycleTime INT,@sql_text1 NVARCHAR(4000),@sql_text2 NVARCHAR(4000),@sql_text3 NVARCHAR(4000)
					DECLARE @error INT ;
					SET @recycleTime=1;   --定义循环次数  
					SET @sql_count = 'select @ii=count(*) from '+@tablename+' where '+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+''''; -- 查增量数据count  应当把配置表中的lastSyncTime设置为当前系统时间
					EXEC sp_executesql @sql_count,N'@ii INT OUT',@ii OUT  --获取总的记录数
					PRINT 'KEWENJIE-31 根据sku更新search表的条数'+@sql_count;
					SET @i = @ii;			 --把总的记录数赋给@i变量			 
					SET @num=@i%2000		--看除数是否为0
					IF(@num=0)
						BEGIN
							SET @count=@i/2000  --除数为0 则不加1  -- A / 2000 =  =  次数
							print @tablename+'需要更新的条数：'
							print @count;
						END		
					ELSE
						BEGIN
							SET @count=@i/2000+1
							print @count;
						END
					WHILE(@recycleTime<=@count)
						BEGIN
							BEGIN TRY
								----在加一个判断  原因是rsds..ZcyGovPriceListItemSku  无salePrice， 是 Price
								DECLARE @sp_price1 NVARCHAR(40)
								IF(@tablename='rsds..ZcyGovPriceListItemSku')
									BEGIN
										SET @sp_price1='a.Price';
									END
								ELSE
									BEGIN
										SET @sp_price1='a.SalePrice';
									END
								--执行根据sku表的数据去更新search  SQL--
								---------根据sku表的数据去更新search SQL   START------------
								SET @sql_text1='select * from '+ @tablename+ ' where '+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+''''
								SET @sql_text2='select *,ROW_NUMBER() over (order by itemSkuId) as rowNum  from('+@sql_text1+') TT'
								SET @sql_text3='update rsds..TChannelSkuPriceSearch set  itemId = a.ItemId,itemSkuId=a.ItemSkuId,salePrice='+@sp_price1+',updateTime=CONVERT(varchar,GETDATE(),120)
										from rsds..TChannelSkuPriceSearch b,('+@sql_text2+') a '+'where a.ItemSkuId= b.itemSkuId and b.channelId ='''+@channelId+'''';
								PRINT 'KEWENJIE---49 打印根据sku表的数据去更新search的SQL '
								PRINT @sql_text3;
								IF(@channelIdField is not null and @channelIdField <> '')
									BEGIN 
										SET @sql_text3=@sql_text3+' and a.'+@channelIdField+'='+(select quotename(@channelId,''''));
									END
								SET @sql_text3=@sql_text3+ ' and a.rowNum >='+ cast((@recycleTime-1)*2000 as varchar) +' and a.rowNum <'+ cast(@recycleTime*2000 as varchar);
								--------根据sku表的数据去更新search  SQL    END-------------
								EXEC(@sql_text3);
								PRINT @sql_text3;
								SET @recycleTime=@recycleTime+1;
							END TRY
							BEGIN CATCH
								PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS VARCHAR(10));
								PRINT 'Error Serverity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
								PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
								PRINT 'Error TableName:  '+@tablename;
								PRINT 'Error Procedure: ' + ERROR_PROCEDURE();
								PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
								PRINT 'Error Message: ' + ERROR_MESSAGE();
							    INSERT  INTO rsds..TchannelErrorLog
										(	ErrorNumber ,		  --错误编号
											ErrorSeverity ,		  --错误级别
											ErrorState ,
											ErrorTableName,
											ErrorProcedure ,
											ErrorLine ,
											ErrorMessage ,
											ErrorDate
										)
								VALUES  (	ERROR_NUMBER() ,
											ERROR_SEVERITY() ,
											ERROR_STATE() ,
											@tablename,
											ERROR_PROCEDURE() ,
											ERROR_LINE() ,
											ERROR_MESSAGE() ,
											GETDATE()
										)
							SET @recycleTime=@recycleTime+1;
							continue;
							END CATCH
						END
					---接下来要做的就是进行插入操作
					DECLARE @sql_text4 nvarchar(4000);
					DECLARE @sql_count1 nvarchar(4000);      --查询出总记录数的sql
					DECLARE @num1 int,@count1 int,@recycleTime1 int; 
					DECLARE @i2 int; --总的记录数

					print 'kwj_start_02_  '+@tableType+' '+@tablename ;
					-- 声明一个变量  目的就是有的专区是专享的，就不需要在searchConfig表中设置channelIdField字段 定义变量的好处是可以提高代码复用
					DECLARE @sp_channelField NVARCHAR(50);
					IF(@channelIdField is not null and @channelIdField <> '')
						BEGIN
							SET @sp_channelField='a.'+@channelIdField;
						END;
					ELSE
						BEGIN
							SET @sp_channelField=''''+@channelId+''''
						END;
					------查询出存在sku表中，但不存在skusearch表的记录数  start-----------
					--定义一个变量去转换SalePrice
					DECLARE @salePrice VARCHAR(400);
					DECLARE @case varchar(400);
					print '打印出售价的类型'+@salePriceType;
					IF(@salePriceType is not null and @salePriceType <> '')
						BEGIN
							IF(@salePriceType='varchar')    --如果是varchar类型  不用转换
								BEGIN
									SET @salePrice='cast(SalePrice as numeric(9,2)) SalePrice '
									SET @case = 'and a.SalePrice !='''' and a.SalePrice is not null ';
								END
							IF(@salePriceType='decimal')							--如果单纯是varchar类型
								BEGIN
									SET @salePrice='cast(SalePrice as numeric(9,2)) SalePrice'
									IF(@tablename='rsds..ZcyGovPriceListItemSku')
										BEGIN
											SET @salePrice='cast(Price as numeric(9,2)) Price'
											SET @case = 'and a.Price is not null'
										END
									ELSE
										BEGIN
											SET @case = 'and a.SalePrice is not null'
										END
								END
							IF(@salePriceType='numeric')
								BEGIN
									SET @salePrice='SalePrice' ;
									SET @case = 'and a.SalePrice is not null'
								END
						END
					print @case;
					SET @sql_count1='select @i2=count(*) from (select '+@sp_channelField+' as channelId,a.ItemId,a.ItemSkuId from '
					+@tablename+' a where not exists(select * from rsds..TChannelSkuPriceSearch b where a.itemSkuId=b.ItemSkuId and b.channelId='+(select quotename(@channelId,''''))+') '+@case+' and a.Status= ' +(select quotename('A',''''))+ ') m;'
					------------------------------------------------------end-------------
					PRINT 'kewenjie-83 执行出存在sku表中但不存在search表的记录数的SQL ';
					PRINT @sql_count1;
					EXEC sp_executesql @sql_count1,N'@i2 INT OUT',@i2 OUT
					PRINT '执行出存在sku表中但不存在search表的记录数 '
					PRINT @i2;
					SET @recycleTime1=1   --定义循环次数
					SET @num1=(select @i2%2000)		--看除数是否为0
						IF(@num1=0)
							BEGIN
								SET @count1=(select @i2/2000);  --除数为0 则不加1
							END				
						ELSE
							BEGIN
								SET @count1=(select @i2/2000+1);
							END
					PRINT '需要插入的总的页数';
					PRINT @count1;
					---------准备插入存在sku表中 而不存在search表中数据的sql语句  START ------
					
					----在加一个判断  原因是rsds..ZcyGovPriceListItemSku  无salePrice， 是 Price
					DECLARE @sp_price NVARCHAR(400)
					IF(@tablename='rsds..ZcyGovPriceListItemSku')
						BEGIN
							SET @sp_price='a.Price';
						END
					ELSE
						BEGIN
							SET @sp_price='a.SalePrice';
						END

					----在加一个判断  原因是rsds..ZcyGovPriceListItemSku  无salePrice， 是 Price
					DECLARE @sp_price2 NVARCHAR(400)
					IF(@tablename='rsds..ZcyGovPriceListItemSku')
						BEGIN
							SET @sp_price2='Price';
						END
					ELSE
						BEGIN
							SET @sp_price2='SalePrice';
						END
					print '转换后的类型------'+@salePrice;
					print '总的页数' 
					print @count1
					WHILE(@recycleTime1<=@count1)
						BEGIN
							BEGIN TRY  
								print '循环次数'
								PRINT @recycleTime1;
								SET @sql_text4 = 'insert into rsds..TChannelSkuPriceSearch( ItemId,ItemSkuId,SalePrice,channelId) select ItemId,ItemSkuId,'+@salePrice+',channelId from '
								+
									'(select channelId,ItemId,ItemSkuId,'+@sp_price2+' from(select *,ROW_NUMBER() over (order by itemskuid) as rowNum from '
										+'('+
											'select '+@sp_channelField+' as channelId,a.ItemId,a.ItemSkuId,'+@sp_price+ ' from '+@tablename+' a where not exists(select * from rsds..TChannelSkuPriceSearch b where a.itemSkuId=b.ItemSkuId and b.channelId= '+@sp_channelField+''
										+')'+@case+' and a.Status= ' +(select quotename('A',''''))+' ) as m'
										+')a where a.rowNum>=0 and a.rowNum <2000)TT'
								print @sp_channelField
								---------准备插入存在sku表中 而不存在search表中数据的sql语句  END ------
								PRINT 'KEWENJIE-106 插入存在sku表中 而不存在search表中数据的sql语句 ';
								PRINT @sql_text4; 
								EXEC(@sql_text4);
								SET @recycleTime1 +=1;
								PRINT @recycleTime1;
							END TRY
							BEGIN CATCH 
								--SELECT error_number() as '柯文杰--135error_number' ,error_message() as error_message
								print 'txl_01 '+@tablename ;
								-- todo 将出错信息写入日志表
								PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS VARCHAR(10));
								PRINT 'Error Serverity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
								PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
								PRINT 'Error Procedure: ' + ERROR_PROCEDURE();
								PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
								PRINT 'Error Message: ' + ERROR_MESSAGE();
								INSERT  INTO rsds..TchannelErrorLog
										(	ErrorNumber ,
											ErrorSeverity ,
											ErrorState ,
											ErrorTableName,
											ErrorProcedure ,
											ErrorLine ,
											ErrorMessage ,
											ErrorDate
										)
								VALUES  (	ERROR_NUMBER() ,
											ERROR_SEVERITY() ,
											ERROR_STATE() ,
											@tablename,
											ERROR_PROCEDURE() ,
											ERROR_LINE() ,
											ERROR_MESSAGE() ,
											GETDATE()
										)
							END CATCH;
						END;
						--更新research的lastSyncTime
					update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=1 and tableName=@tablename
			--去除在rsds..tchannelskupriceSearch状态为p的数据******start*******************************************************************************************
			print '删除rsds..tchannelSkupriceSearch的状态为p的*******'
			DECLARE @sp_status nvarchar(4000);
			IF(@channelIdField is not null)
				begin
					set @sp_status='delete rsds..TChannelSkuPriceSearch from rsds..TChannelSkuPriceSearch  a inner join '+@tableName+' b on a.channelId=b.'+@channelIdField+
					' and a.itemSkuId = b.ItemSkuId and b.Status='+ (select quotename('p',''''))
				end;
			ELSE
				begin
					set @sp_status='delete rsds..TChannelSkuPriceSearch from rsds..TChannelSkuPriceSearch  a inner join '+@tableName+' b on a.channelId='+(select quotename(@channelId,''''))+
					' and a.itemSkuId = b.ItemSkuId and b.Status='+ (select quotename('p',''''))
				end;
				exec (@sp_status);
			--去除在rsds..tchannelskupriceSearch状态为p的数据********end*******************************************************************************************
			END;  --@tableType=1的结束标志

				--@tableType =2
			ElSE IF(@tableType=2)
				BEGIN
					DECLARE @sp_Update nvarchar(4000)
					DECLARE @ii3 int,@count2 int,@num2 int,@recycleTime2 int
					DECLARE @sp_Count nvarchar(4000)
					SET @sp_count = 'select @ii3=count(*) from '+@tablename+' t where t.'+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+'''';
					EXEC sp_executesql @sp_Count,N'@ii3 INT OUT',@ii3 OUT
					print @sp_count;
			-----		IF(@ii3>0)				--如果这个需要更新的条数大于0 则更新config表中的syncTime
			-----			BEGIN
			-----				update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=2 and tableName=@tablename
			-----			END
					SET @recycleTime2=1   --定义循环次数
					SET @num2=(select @ii3%2000)		--看除数是否为0
						IF(@num2=0)
							BEGIN
								SET @count2=(select @ii3/2000);  --除数为0 则不加1
							END				
						ELSE
							BEGIN
								SET @count2=(select @ii3/2000+1);
							END
					print '总的页数--'
					print @count2;
					WHILE(@recycleTime2<=@count2)
						BEGIN
							BEGIN TRY
							IF(@tablename='rspm..TItem')
								BEGIN
									SET @sp_Update='update b set  classId1 = a.ClassId1,classId2=a.ClassId2,classId3=a.ClassId3,updateTime=CONVERT(varchar,GETDATE(),120) from rsds..TChannelSkuPriceSearch b,'+
								'('+
								'select *, ROW_NUMBER() OVER(Order by itemId ) AS RowNumber from'+
									'(select * from '+@tablename+' t where t.'+@changeDateField
									+'>'''+convert(varchar(20),@lastSyncTime,120)+''')t)as a where b.channelId='''+@channelId+''' and b.itemId=a.ItemId and RowNumber BETWEEN '+cast((@recycleTime2-1)*2000 as varchar)+' and '+cast(@recycleTime2*2000 as varchar)
								END
							ELSE IF(@tablename='rspm..TStateGridCommodity' or @tablename='rspm..TCEECCommodity')
								BEGIN
									SET @sp_Update='update b set  classId1 = a.CommodityCatId1,classId2=a.CommodityCatId2,classId3=a.CommodityCatId3,updateTime=CONVERT(varchar,GETDATE(),120) from rsds..TChannelSkuPriceSearch b,'+
									'('+
									'select *, ROW_NUMBER() OVER(Order by itemSkuId ) AS RowNumber from'+
										'(select * from '+@tablename+' t where t.'+@changeDateField
									+'>'''+convert(varchar(20),@lastSyncTime,120)+''')t)as a where b.channelId='''+@channelId+''' and a.ItemSkuId=b.itemSkuId and RowNumber BETWEEN '+cast((@recycleTime2-1)*2000 as varchar)+' and '+cast(@recycleTime2*2000 as varchar)
								END
							ELSE
								BEGIN
									SET @sp_Update='update b set  classId1 = a.ClassId1,classId2=a.ClassId2,classId3=a.ClassId3,updateTime=CONVERT(varchar,GETDATE(),120) from rsds..TChannelSkuPriceSearch b,'+
									'('+
									'select *, ROW_NUMBER() OVER(Order by itemSkuId ) AS RowNumber from'+
									'(select * from '+@tablename+' t where t.'+@changeDateField
									+'>'''+convert(varchar(20),@lastSyncTime,120)+''')t)as a where b.channelId='''+@channelId+''' and a.ItemSkuId=b.itemSkuId and RowNumber BETWEEN '+cast((@recycleTime2-1)*2000 as varchar)+' and '+cast(@recycleTime2*2000 as varchar)
								END
								print @sp_Update;
								EXEC (@sp_Update);
								SET @recycleTime2 +=1;
								update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=2 and tableName=@tablename;
							END TRY
							BEGIN CATCH 
								PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS VARCHAR(10));
								PRINT 'Error Serverity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
								PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
								PRINT 'Error Procedure: ' + ERROR_PROCEDURE();
								PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
								PRINT 'Error Message: ' + ERROR_MESSAGE();
								INSERT  INTO rsds..TchannelErrorLog
										(	ErrorNumber ,
											ErrorSeverity ,
											ErrorState ,
											ErrorTableName,
											ErrorProcedure ,
											ErrorLine ,
											ErrorMessage ,
											ErrorDate
										)
								VALUES  (	ERROR_NUMBER() ,
											ERROR_SEVERITY() ,
											ERROR_STATE() ,
											@tablename,
											ERROR_PROCEDURE() ,
											ERROR_LINE() ,
											ERROR_MESSAGE() ,
											GETDATE()
										)
								SET @recycleTime2 +=1;
								continue;
							END CATCH;
						END;
				END
			FETCH NEXT FROM rsdsCur INTO @tableName,@lastSyncTime,@tableType,@changeDateField,@channelIdField,@channelId,@salePriceType;    --继续用游标读取下一个数据  
		END  
	CLOSE rsdsCur;										--关闭游标
	DEALLOCATE rsdsCur;	
	--步骤三 专区价格限制  限制量不大 直接一次更新  理清什么是增量 当价格限制表的更新时间大于lastSyncTime
	BEGIN
		print '更新专区价格限制：'
		update b set limitPrice=a.maxPriceLimit from rsds..TChannelSkuPriceSearch b,  rsds..TChannelSkuPriceLimit a  where a.ItemSkuId= b.itemSkuId and b.channelId =a.channelId and a.updateTime>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=4);
		print 'hello world'
		IF ((select count(1) from rsds..TChannelSkuPriceSearch b, rsds..TChannelSkuPriceLimit a  where a.ItemSkuId= b.itemSkuId and b.channelId =a.channelId and a.updateTime>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=4))>0)
			BEGIN
				update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=4;
			END
	END
	--专区价格限制同步完成之后，应当让rsds..tchannelskupriceSearchConfig表中的lastSyncTime=当前系统时间where tabletype=3的时候
	--步骤四  类别级别的限制   使用三条update的sql语句  增量是什么 根据时间大于rsds..tchannelskupriceSearchConfig表中的lastSyncTime获取
	--这是更新classId1
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and IsNull(a.ClassId2,'')='' and IsNull(a.ClassId3,'')='' and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--这是更新classId1 和classId2
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and a.ClassId2=b.classId2 and IsNull(a.ClassId3,'')='' and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--这是更新classId1 和classId2 和classId3
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and a.ClassId2=b.classId2 and a.ClassId3=b.classId3 and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--步骤五 商城面价  来源于rspm的itemsku 同样要根据时间>lastsyncTime作为增量
	BEGIN
		update b set marketPrice=a.SalePrice from rsds..TItemSku a,rsds..TChannelSkuPriceSearch b where a.ItemSkuId=b.itemSkuId and b.marketPrice is null
	END
	--步骤六 添加专区名称   --更具config表去更新search的专区名称
	--根据searchconfig的channelid去更新b表的channelName  什么是增量  当search表中的channelName为null的时候  
	--注意需要添加tabletype=1的条件约束
	BEGIN
		update b set channelName=a.channelName  from rsds..TChannelSkuPriceSearch b,rsds..TChannelSkuPriceSearchSyncConfig a 
		where a.channelId = b.channelId and b.channelName is null and a.tableType=1;
	END
	--根据itemskuname
	BEGIN
		update b set itemSkuName=a.ItemSkuName from rsds..TChannelSkuPriceSearch b,rspm..TItemSku a where a.ItemSkuId = b.itemSkuId AND b.itemSkuName is null
	END 
END
GO

EXEC SyncDataToSearch;			--执行存储过程——
