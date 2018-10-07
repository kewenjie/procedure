/**
	�޸ļ�¼
	2018/8/30  
		ҵ������: �½��洢����,��ר���۸�����ͬ����TChannelSkuPriceSearch 
		�޸���: ���Ľ�
	2018/9/20
		ҵ������: ͬ������ʱ����ר��SKU������״̬���ж�,��״̬ΪP ��ɾ�� TChannelSkuPriceSearch �ж�Ӧ������
		�޸���: ���Ľ�
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
	--����һ���α�
	DECLARE rsdsCur CURSOR FOR 
	SELECT tableName,lastSyncTime,tableType,changeDateField,channelIdField,channelId,SalePriceType FROM rsds..TChannelSkuPriceSearchSyncConfig --where tableName='rsds..TABCPriceListItemSku';
	OPEN rsdsCur
	FETCH NEXT FROM rsdsCur INTO @tableName,@lastSyncTime,@tableType,@changeDateField,@channelIdField,@channelId,@salePriceType;
	WHILE (@@FETCH_STATUS <>-1)                        --�α��ȡ��һ�����ݵ�״̬��0��ʾ��ȡ�ɹ�  
		BEGIN
			print 'txl_start_01_  '+@tableType+' '+@tablename;
			--��ʼд���Ĵ���
			IF(@tableType=1)
				BEGIN
					DECLARE @i INT,@ii INT,@num INT,@sql_count NVARCHAR(200),@count INT,@recycleTime INT,@sql_text1 NVARCHAR(4000),@sql_text2 NVARCHAR(4000),@sql_text3 NVARCHAR(4000)
					DECLARE @error INT ;
					SET @recycleTime=1;   --����ѭ������  
					SET @sql_count = 'select @ii=count(*) from '+@tablename+' where '+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+''''; -- ����������count  Ӧ�������ñ��е�lastSyncTime����Ϊ��ǰϵͳʱ��
					EXEC sp_executesql @sql_count,N'@ii INT OUT',@ii OUT  --��ȡ�ܵļ�¼��
					PRINT 'KEWENJIE-31 ����sku����search�������'+@sql_count;
					SET @i = @ii;			 --���ܵļ�¼������@i����			 
					SET @num=@i%2000		--�������Ƿ�Ϊ0
					IF(@num=0)
						BEGIN
							SET @count=@i/2000  --����Ϊ0 �򲻼�1  -- A / 2000 =  =  ����
							print @tablename+'��Ҫ���µ�������'
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
								----�ڼ�һ���ж�  ԭ����rsds..ZcyGovPriceListItemSku  ��salePrice�� �� Price
								DECLARE @sp_price1 NVARCHAR(40)
								IF(@tablename='rsds..ZcyGovPriceListItemSku')
									BEGIN
										SET @sp_price1='a.Price';
									END
								ELSE
									BEGIN
										SET @sp_price1='a.SalePrice';
									END
								--ִ�и���sku�������ȥ����search  SQL--
								---------����sku�������ȥ����search SQL   START------------
								SET @sql_text1='select * from '+ @tablename+ ' where '+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+''''
								SET @sql_text2='select *,ROW_NUMBER() over (order by itemSkuId) as rowNum  from('+@sql_text1+') TT'
								SET @sql_text3='update rsds..TChannelSkuPriceSearch set  itemId = a.ItemId,itemSkuId=a.ItemSkuId,salePrice='+@sp_price1+',updateTime=CONVERT(varchar,GETDATE(),120)
										from rsds..TChannelSkuPriceSearch b,('+@sql_text2+') a '+'where a.ItemSkuId= b.itemSkuId and b.channelId ='''+@channelId+'''';
								PRINT 'KEWENJIE---49 ��ӡ����sku�������ȥ����search��SQL '
								PRINT @sql_text3;
								IF(@channelIdField is not null and @channelIdField <> '')
									BEGIN 
										SET @sql_text3=@sql_text3+' and a.'+@channelIdField+'='+(select quotename(@channelId,''''));
									END
								SET @sql_text3=@sql_text3+ ' and a.rowNum >='+ cast((@recycleTime-1)*2000 as varchar) +' and a.rowNum <'+ cast(@recycleTime*2000 as varchar);
								--------����sku�������ȥ����search  SQL    END-------------
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
										(	ErrorNumber ,		  --������
											ErrorSeverity ,		  --���󼶱�
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
					---������Ҫ���ľ��ǽ��в������
					DECLARE @sql_text4 nvarchar(4000);
					DECLARE @sql_count1 nvarchar(4000);      --��ѯ���ܼ�¼����sql
					DECLARE @num1 int,@count1 int,@recycleTime1 int; 
					DECLARE @i2 int; --�ܵļ�¼��

					print 'kwj_start_02_  '+@tableType+' '+@tablename ;
					-- ����һ������  Ŀ�ľ����е�ר����ר��ģ��Ͳ���Ҫ��searchConfig��������channelIdField�ֶ� ��������ĺô��ǿ�����ߴ��븴��
					DECLARE @sp_channelField NVARCHAR(50);
					IF(@channelIdField is not null and @channelIdField <> '')
						BEGIN
							SET @sp_channelField='a.'+@channelIdField;
						END;
					ELSE
						BEGIN
							SET @sp_channelField=''''+@channelId+''''
						END;
					------��ѯ������sku���У���������skusearch��ļ�¼��  start-----------
					--����һ������ȥת��SalePrice
					DECLARE @salePrice VARCHAR(400);
					DECLARE @case varchar(400);
					print '��ӡ���ۼ۵�����'+@salePriceType;
					IF(@salePriceType is not null and @salePriceType <> '')
						BEGIN
							IF(@salePriceType='varchar')    --�����varchar����  ����ת��
								BEGIN
									SET @salePrice='cast(SalePrice as numeric(9,2)) SalePrice '
									SET @case = 'and a.SalePrice !='''' and a.SalePrice is not null ';
								END
							IF(@salePriceType='decimal')							--���������varchar����
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
					PRINT 'kewenjie-83 ִ�г�����sku���е�������search��ļ�¼����SQL ';
					PRINT @sql_count1;
					EXEC sp_executesql @sql_count1,N'@i2 INT OUT',@i2 OUT
					PRINT 'ִ�г�����sku���е�������search��ļ�¼�� '
					PRINT @i2;
					SET @recycleTime1=1   --����ѭ������
					SET @num1=(select @i2%2000)		--�������Ƿ�Ϊ0
						IF(@num1=0)
							BEGIN
								SET @count1=(select @i2/2000);  --����Ϊ0 �򲻼�1
							END				
						ELSE
							BEGIN
								SET @count1=(select @i2/2000+1);
							END
					PRINT '��Ҫ������ܵ�ҳ��';
					PRINT @count1;
					---------׼���������sku���� ��������search�������ݵ�sql���  START ------
					
					----�ڼ�һ���ж�  ԭ����rsds..ZcyGovPriceListItemSku  ��salePrice�� �� Price
					DECLARE @sp_price NVARCHAR(400)
					IF(@tablename='rsds..ZcyGovPriceListItemSku')
						BEGIN
							SET @sp_price='a.Price';
						END
					ELSE
						BEGIN
							SET @sp_price='a.SalePrice';
						END

					----�ڼ�һ���ж�  ԭ����rsds..ZcyGovPriceListItemSku  ��salePrice�� �� Price
					DECLARE @sp_price2 NVARCHAR(400)
					IF(@tablename='rsds..ZcyGovPriceListItemSku')
						BEGIN
							SET @sp_price2='Price';
						END
					ELSE
						BEGIN
							SET @sp_price2='SalePrice';
						END
					print 'ת���������------'+@salePrice;
					print '�ܵ�ҳ��' 
					print @count1
					WHILE(@recycleTime1<=@count1)
						BEGIN
							BEGIN TRY  
								print 'ѭ������'
								PRINT @recycleTime1;
								SET @sql_text4 = 'insert into rsds..TChannelSkuPriceSearch( ItemId,ItemSkuId,SalePrice,channelId) select ItemId,ItemSkuId,'+@salePrice+',channelId from '
								+
									'(select channelId,ItemId,ItemSkuId,'+@sp_price2+' from(select *,ROW_NUMBER() over (order by itemskuid) as rowNum from '
										+'('+
											'select '+@sp_channelField+' as channelId,a.ItemId,a.ItemSkuId,'+@sp_price+ ' from '+@tablename+' a where not exists(select * from rsds..TChannelSkuPriceSearch b where a.itemSkuId=b.ItemSkuId and b.channelId= '+@sp_channelField+''
										+')'+@case+' and a.Status= ' +(select quotename('A',''''))+' ) as m'
										+')a where a.rowNum>=0 and a.rowNum <2000)TT'
								print @sp_channelField
								---------׼���������sku���� ��������search�������ݵ�sql���  END ------
								PRINT 'KEWENJIE-106 �������sku���� ��������search�������ݵ�sql��� ';
								PRINT @sql_text4; 
								EXEC(@sql_text4);
								SET @recycleTime1 +=1;
								PRINT @recycleTime1;
							END TRY
							BEGIN CATCH 
								--SELECT error_number() as '���Ľ�--135error_number' ,error_message() as error_message
								print 'txl_01 '+@tablename ;
								-- todo ��������Ϣд����־��
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
						--����research��lastSyncTime
					update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=1 and tableName=@tablename
			--ȥ����rsds..tchannelskupriceSearch״̬Ϊp������******start*******************************************************************************************
			print 'ɾ��rsds..tchannelSkupriceSearch��״̬Ϊp��*******'
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
			--ȥ����rsds..tchannelskupriceSearch״̬Ϊp������********end*******************************************************************************************
			END;  --@tableType=1�Ľ�����־

				--@tableType =2
			ElSE IF(@tableType=2)
				BEGIN
					DECLARE @sp_Update nvarchar(4000)
					DECLARE @ii3 int,@count2 int,@num2 int,@recycleTime2 int
					DECLARE @sp_Count nvarchar(4000)
					SET @sp_count = 'select @ii3=count(*) from '+@tablename+' t where t.'+@changeDateField+'>'''+convert(varchar(20),@lastSyncTime,120)+'''';
					EXEC sp_executesql @sp_Count,N'@ii3 INT OUT',@ii3 OUT
					print @sp_count;
			-----		IF(@ii3>0)				--��������Ҫ���µ���������0 �����config���е�syncTime
			-----			BEGIN
			-----				update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=2 and tableName=@tablename
			-----			END
					SET @recycleTime2=1   --����ѭ������
					SET @num2=(select @ii3%2000)		--�������Ƿ�Ϊ0
						IF(@num2=0)
							BEGIN
								SET @count2=(select @ii3/2000);  --����Ϊ0 �򲻼�1
							END				
						ELSE
							BEGIN
								SET @count2=(select @ii3/2000+1);
							END
					print '�ܵ�ҳ��--'
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
			FETCH NEXT FROM rsdsCur INTO @tableName,@lastSyncTime,@tableType,@changeDateField,@channelIdField,@channelId,@salePriceType;    --�������α��ȡ��һ������  
		END  
	CLOSE rsdsCur;										--�ر��α�
	DEALLOCATE rsdsCur;	
	--������ ר���۸�����  ���������� ֱ��һ�θ���  ����ʲô������ ���۸����Ʊ�ĸ���ʱ�����lastSyncTime
	BEGIN
		print '����ר���۸����ƣ�'
		update b set limitPrice=a.maxPriceLimit from rsds..TChannelSkuPriceSearch b,  rsds..TChannelSkuPriceLimit a  where a.ItemSkuId= b.itemSkuId and b.channelId =a.channelId and a.updateTime>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=4);
		print 'hello world'
		IF ((select count(1) from rsds..TChannelSkuPriceSearch b, rsds..TChannelSkuPriceLimit a  where a.ItemSkuId= b.itemSkuId and b.channelId =a.channelId and a.updateTime>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=4))>0)
			BEGIN
				update rsds..TChannelSkuPriceSearchSyncConfig set lastSyncTime=CONVERT(varchar,GETDATE(),120) where tableType=4;
			END
	END
	--ר���۸�����ͬ�����֮��Ӧ����rsds..tchannelskupriceSearchConfig���е�lastSyncTime=��ǰϵͳʱ��where tabletype=3��ʱ��
	--������  ��𼶱������   ʹ������update��sql���  ������ʲô ����ʱ�����rsds..tchannelskupriceSearchConfig���е�lastSyncTime��ȡ
	--���Ǹ���classId1
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and IsNull(a.ClassId2,'')='' and IsNull(a.ClassId3,'')='' and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--���Ǹ���classId1 ��classId2
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and a.ClassId2=b.classId2 and IsNull(a.ClassId3,'')='' and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--���Ǹ���classId1 ��classId2 ��classId3
	BEGIN
		update b set discountLimit=a.discount,grossProfit=a.grossProfit
		from rsds..TChannelSkuPriceSearch b ,rsds..TChannelClassProfit a
		where a.ChannelId=b.channelId and a.ClassId1=b.classId1 and a.ClassId2=b.classId2 and a.ClassId3=b.classId3 and a.DateChanged>(select lastSyncTime from rsds..TChannelSkuPriceSearchSyncConfig where tableType=3);
	END
	--������ �̳����  ��Դ��rspm��itemsku ͬ��Ҫ����ʱ��>lastsyncTime��Ϊ����
	BEGIN
		update b set marketPrice=a.SalePrice from rsds..TItemSku a,rsds..TChannelSkuPriceSearch b where a.ItemSkuId=b.itemSkuId and b.marketPrice is null
	END
	--������ ���ר������   --����config��ȥ����search��ר������
	--����searchconfig��channelidȥ����b���channelName  ʲô������  ��search���е�channelNameΪnull��ʱ��  
	--ע����Ҫ���tabletype=1������Լ��
	BEGIN
		update b set channelName=a.channelName  from rsds..TChannelSkuPriceSearch b,rsds..TChannelSkuPriceSearchSyncConfig a 
		where a.channelId = b.channelId and b.channelName is null and a.tableType=1;
	END
	--����itemskuname
	BEGIN
		update b set itemSkuName=a.ItemSkuName from rsds..TChannelSkuPriceSearch b,rspm..TItemSku a where a.ItemSkuId = b.itemSkuId AND b.itemSkuName is null
	END 
END
GO

EXEC SyncDataToSearch;			--ִ�д洢���̡���
