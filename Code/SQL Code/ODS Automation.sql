/****** Object:  StoredProcedure [dbo].[ODS_Automation_Script]    Script Date: 26/06/2023 11:40:52 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE    procedure [dbo].[ODS_Automation_Script] 

@i_tableName         varchar(max),
@i_schemaName		varchar(1000),
@StageTable_Prefix	varchar(1000),
@ODS_Prefix			varchar(1000),
@PRIMARY_KEY		varchar(1000),
@ROWHASH_KEY		varchar(1000),
@DATE_UPDATE_KEY	varchar(1000)

as
/*


exec dbo.ODS_Automation_Script
		fma_shareholder,  --TableName
		'clare.',			--SchemaName
		v_Stage_,			--View Name
		ODS_,				-- ODS Indicator
		fma_shareholderId, --primary Key
		row_hash_type2,		
		modifiedon			--WatermarkColumn


	*/


DECLARE @i_Columns varchar(max)
DECLARE @i_SelectStatement varchar(max)
DECLARE @i_Hash varchar(max)
DECLARE @i_StandardFields varchar(2000)

--Declare Stage Table meta data information
DECLARE @s_Columns varchar(max)
DECLARE @stageTableName varchar(200)
DECLARE @StageTableSchema varchar(100)
DECLARE @ODS_ColumnName varchar(max)
DECLARE @ODS_TableName varchar(200)
DECLARE @ODS_SchemaName varchar(100)
DECLARE @ODS_TableSchema varchar(100)
DECLARE @sysUser varchar(100)
DECLARE @RunDate varchar(100)
DECLARE @DelTable varchar(100)
DECLARE @Del_PostFix varchar(100)
DECLARE @Del_tablename varchar(200)


Set @sysUser = (Select SYSTEM_USER)
Set @RunDate = (Select Getdate())
Set @Del_PostFix = '_DEL'

DECLARE @DATEINSERTED varchar(2000)



drop table if exists #SQLStatement
Create table #SQLStatement  (OrderNum int,SQLStatement varchar(max) )


Set @i_StandardFields = ('Date_Inserted datetime, Date_Updated datetime')


/*
DECLARE THE STAGE TABLES AND ODS TABLES NAMES AND PREFIXES


@i_tableName 
@i_schemaName

*/

Declare @DelTableFull nvarchar(200)

Set @stageTableName		= @StageTable_Prefix + @i_tableName
SET @StageTableSchema	= @i_schemaName + @stageTableName
SET	@ODS_TableName		= @ODS_Prefix + @i_tableName
SET @ODS_TableSchema	= @i_schemaName + @ODS_TableName
SET @DelTable			= @i_tableName + @Del_PostFix
SET @DelTableFull       = @i_schemaName +@i_tableName + @Del_PostFix
Set @Del_tablename = 'clare.fma_softdelete_tracking'




IF @DATE_UPDATE_KEY = 'getdate()'
BEGIN
		SET @DATEINSERTED = @DATE_UPDATE_KEY
END
ELSE
BEGIN
		SET @DATEINSERTED = @i_tableName+'.'+ @DATE_UPDATE_KEY
END

drop table if exists  #load_syscolumns

Select tab.name,  '['+cast(col.name as varchar(1000))+']' as column_Name,column_id
Into #load_syscolumns

from sys.tables tab
		inner join
				sys.schemas sch 
					on	tab.schema_id =sch.schema_id
		inner join 
				sys.all_columns col
					on tab.object_id = col.object_id

Where tab.name  = @i_tableName
and col.name not in ( 'ETL_Datetime','versionnumber')


-- 1. Get the columns names from sys tables and sys columns


SET @i_Columns  = (
						--Select  ',' +@i_tableName +'.' + cast(column_name as varchar(1000)) --,cast(answer_numeric_value as varchar(30))
						Select  ','  + cast(column_name as varchar(1000)) --,cast(answer_numeric_value as varchar(30))
						from #load_syscolumns x
						order by column_id
						for XMl PATH (''))

-- 2.  Remove first comma from the xml column names

SET @s_Columns = substring(@i_Columns,2,10000000000)

declare @h_column varchar(max)
declare @sh_column varchar(max)


--3. Create hash columns
SET @h_Column  = (
						--Select  ',' +@i_tableName +'.' + cast(column_name as varchar(1000)) --,cast(answer_numeric_value as varchar(30))
						Select  ','  + cast(column_name as varchar(1000)) --,cast(answer_numeric_value as varchar(30))
						from #load_syscolumns x
						Where		column_name  not like '%idw%'
						order by column_id
						for XMl PATH (''))

-- 4. remove first column from  hash column

SET @sh_column = substring(@h_Column,2,10000000000)

DECLARE @Insert_Columns  varchar(max)

-- 5. Create insert column name for Target
Set @Insert_Columns = ((
						Select  ',' + cast(column_name as varchar(200)) --,cast(answer_numeric_value as varchar(30))
						from #load_syscolumns x
						order by column_id
						for XMl PATH (''))
						)
DECLARE @ins_Columns varchar(max)

Set @ins_Columns = substring(@Insert_Columns,2,10000000000)

Set @i_Hash = ',convert(varbinary(32),hashbytes(''SHA2_256'', ( select ' + @sh_column + ' for xml raw , BINARY BASE64 ))) as row_hash_type2'

--Select * from sys.tables where name = (@stageTableName) 

DECLARE @stageColumns varchar(max)
SET @stageColumns  = ((
						Select  ',' +@i_tableName +'.' + cast(column_name as varchar(200)) --,cast(answer_numeric_value as varchar(30))
						from #load_syscolumns x
						Order by column_id
						for XMl PATH (''))
						)


						Select @stageColumns



Insert into #SQLStatement values (0000, 'Create procedure '+@i_schemaName + 'proc_' + @ODS_TableName  + ' @idw_batchid int as ')

Insert into #SQLStatement values (0001, '   ' )



Insert into #SQLStatement values (0001, '/* Variables passed thru   ' )

Insert into #SQLStatement values (0002 , 'TableName:'+ @i_tableName        )
Insert into #SQLStatement values (0003 , 'schema:'+ @i_schemaName		)
Insert into #SQLStatement values (0004 , 'tablePrefix:'+ @StageTable_Prefix)	
Insert into #SQLStatement values (0005 , 'ODS:'+ @ODS_Prefix			)
Insert into #SQLStatement values (0006 , 'primarykey:'+ @PRIMARY_KEY	)	
Insert into #SQLStatement values (0007 , 'rowhash:'+ @ROWHASH_KEY		)
Insert into #SQLStatement values (0008 , 'datekey:'+ @DATE_UPDATE_KEY	)
Insert into #SQLStatement values (0009 , '                          ')
Insert into #SQLStatement values (0010 , 'exec '+ @i_schemaName + 'proc_'+ @ODS_TableName	)

Insert into #SQLStatement values (0011 , '--------------------------------     ' 	)
Insert into #SQLStatement values (0012 , 'Only run this statement you want to re-create the procedure'	)

Insert into #SQLStatement values (0013 , 'exec dbo.ODS_Automation_Script')

Insert into #SQLStatement values (0014 ,     +@i_tableName        )
Insert into #SQLStatement values (0015 , ',''' +@i_schemaName + ''''		)
Insert into #SQLStatement values (0016 , ',' +@StageTable_Prefix)	
Insert into #SQLStatement values (0017 , ',' +@ODS_Prefix			)
Insert into #SQLStatement values (0018 , ',' +@PRIMARY_KEY	)	
Insert into #SQLStatement values (0019 , ',' +@ROWHASH_KEY		)
Insert into #SQLStatement values (0020 , ',' +@DATE_UPDATE_KEY	)



Insert into #SQLStatement values (00021 , '    ')
Insert into #SQLStatement values (00022 , '    ')

Insert into #SQLStatement values (00023 , 'Procedure Create by     ' + @sysUser )
Insert into #SQLStatement values (00024 , 'Procedure Create on     ' + @RunDate )

Insert into #SQLStatement values (0050 , '*/'	)


Insert into #SQLStatement values (0099 , '                          ')


/* 



this section contains the audit statement



*/

-- Get ODS Table Name
------PROCESS LOOKS REFERENCES VIEWS

if @stageTableName = (Select name from sys.tables where name = (@ODS_TableSchema) )
begin
		
				Insert into #SQLStatement values (5000,'ODS Table already exists and cant be regenerated' ) --+ @i_Hash + ' FROM '+  @i_schemaName + @i_tableName
	
end 

else 
		Begin
		 		--Insert into #SQLStatement values (5001,'create view ' + @i_schemaName + @stageTableName + ' as ' ) --+ @i_Hash + ' FROM '+  @i_schemaName + @i_tableName

				-- Create ODS table if the table doesn't exist
				Insert into #SQLStatement values (5001,'IF Object_ID ('''+@ODS_TableSchema+''') IS NULL')
				Insert into #SQLStatement values (5001,'BEGIN')
				--Insert into #SQLStatement values (5001,'/*')
			--	Insert into #SQLStatement values (5002,'Only run this section to code of the ODS tables doesn exist')
				Insert into #SQLStatement values (5003,'      ')
				Insert into #SQLStatement values (5004,'')
				Insert into #SQLStatement values (5005,'Select ' +@s_Columns)
				Insert into #SQLStatement values (5006,'		' +@i_Hash)
				Insert into #SQLStatement values (5007,   '		,1 as idw_current, 1 as idw_version, '+ @DATE_UPDATE_KEY + ' as idw_version_start_date, cast(''2999-01-01'' as datetime) as idw_version_end_date, ''I'' as row_change_type')
				Insert into #SQLStatement values (5008,   '		,1 as idw_batchid, 0 as idw_update_batchid')


				Insert into #SQLStatement values (5009,'Into '+ @ODS_TableSchema)
				Insert into #SQLStatement values (5010,'		FROM ')
				Insert into #SQLStatement values (5011,'		'+ @i_schemaName + @i_tableName)
				Insert into #SQLStatement values (5011,'WHERE 1=0')
				--Insert into #SQLStatement values (5012,'truncate table '+@ODS_TableSchema)
				Insert into #SQLStatement values (5011,'END	')
				--Insert into #SQLStatement values (5013,'*/')

				Insert into #SQLStatement values (5014,'     ')
				Insert into #SQLStatement values (5014,'     ')
				Insert into #SQLStatement values (5014,'     ')

				--Create temp table and then insert into temp table
				Insert into #SQLStatement values (5015,'--Creates temp table as pre table to insert into ODS table')
				Insert into #SQLStatement values (5016,'     ')
				Insert into #SQLStatement values (5017,'Select ' +@s_Columns)
				Insert into #SQLStatement values (5018,'		'+@i_Hash)
				Insert into #SQLStatement values (5019,   '		,1 as idw_current, 1 as idw_version, '+ @DATE_UPDATE_KEY + ' as idw_version_start_date, ''2999-01-01'' as idw_version_end_date, ''I'' as row_change_type')
				Insert into #SQLStatement values (5020,   '		,@idw_batchid as idw_batchid, 0 as idw_update_batchid')


				Insert into #SQLStatement values (5021,'Into #'+ @i_tableName)
				Insert into #SQLStatement values (5022,'		FROM ')
				Insert into #SQLStatement values (5023,'		'+ @i_schemaName + @i_tableName)




Insert into #SQLStatement values (6000, '                    ')	
Insert into #SQLStatement values (6000, '                    ')	
Insert into #SQLStatement values (6000, '                    ')	
Insert into #SQLStatement values (6000, '-- CREATE HISTORICAL RECORD WHEN RECORD CHANGES     ')	
Insert into #SQLStatement values (6001, 'UPDATE ' + @ODS_TableSchema		)															--  clare.ODS_Filteredfma_licenceapplication
Insert into #SQLStatement values (6002, 'SET		idw_current = 0,')
Insert into #SQLStatement values (6003,  'idw_version_end_date = dateadd(ss,-1,' + @DATEINSERTED +')')
Insert into #SQLStatement values (6004,  ',row_change_type = ''U''')
Insert into #SQLStatement values (6005,  ',idw_update_batchid = @idw_batchid')
Insert into #SQLStatement values (6005,	'FROM	'  +  @ODS_TableSchema + ' ' +@ODS_TableName	)										-- clare.ODS_Filteredfma_licenceapplication a 
Insert into #SQLStatement values (6006, 'INNER JOIN')
Insert into #SQLStatement values (6007,  '#'+@i_tableName +' '+ @i_tableName)														--clare.zzStage_Filteredfma_licenceapplication b
Insert into #SQLStatement values (6008,	'on  ' + @ODS_TableName+'.'+ @PRIMARY_KEY + ' = ' + @i_tableName+'.'+@PRIMARY_KEY)		--  LicenceApplication_BK = b.LicenceApplication_BK
Insert into #SQLStatement values (6009,  'where ' +@i_tableName +'.row_hash_type2 <> ' +@ODS_TableName +'.row_hash_type2')
Insert into #SQLStatement values (6010, 'and	' + @ODS_TableName+'.idw_current = 1')



	
				
Insert into #SQLStatement values (7000, '                                         ' )
Insert into #SQLStatement values (7000, '                                         ' )
Insert into #SQLStatement values (7000, '-- INSERT INTO ODS TABLE           ' )
Insert into #SQLStatement values (7000, '                                         ' )
Insert into #SQLStatement values (7001, 'Insert into ' +@ODS_TableSchema +'(' )
Insert into #SQLStatement values (7002, '                     '+ @s_Columns)

Insert into #SQLStatement values (7004,   '					,idw_current,idw_version,idw_version_start_date,idw_version_end_date,row_change_type')
Insert into #SQLStatement values (7005,   '					,idw_batchid, idw_update_batchid,row_hash_type2')

Insert into #SQLStatement values (7006, ')')
Insert into #SQLStatement values (7007, 'Select '  + substring(@stageColumns,2,100000) )
Insert into #SQLStatement values (7008,   '			,' +  @i_tableName +'.idw_current,'+@i_tableName +'.idw_version,'+@i_tableName +'. idw_version_start_date, ''2999-01-01'' as idw_version_end_date, ''I'' as row_change_type')
Insert into #SQLStatement values (7009,   '			,'+@i_tableName +'.idw_batchid, 0 as idw_update_batchid,'+@i_tableName +'.row_hash_type2')
Insert into #SQLStatement values (7010, 'FROM #'  + @i_tableName +' '+  @i_tableName )
Insert into #SQLStatement values (7011, '			LEFT OUTER JOIN' )
Insert into #SQLStatement values (7012, @ODS_TableSchema + ' ' + @ODS_TableName)
Insert into #SQLStatement values (7013, '			ON '  + @i_tableName +'.row_hash_type2 = ' + @ODS_TableName + '.row_hash_type2')
Insert into #SQLStatement values (7014, '			Where ' + @ODS_TableName + '.row_hash_type2 is null')




--Insert into #SQLStatement values (8000, '                    ')	
--Insert into #SQLStatement values (8001, '                    ')	
--Insert into #SQLStatement values (8002, '                    ')	
--Insert into #SQLStatement values (8003, '--Update delete recprds to inactive   ')	
--Insert into #SQLStatement values (8004, 'UPDATE ' + @ODS_TableSchema		)															--  clare.ODS_Filteredfma_licenceapplication
--Insert into #SQLStatement values (8006,  'SET idw_version_end_date = getdate()')
--Insert into #SQLStatement values (8007,  ',row_change_type = ''D''')
--Insert into #SQLStatement values (8008,  ',idw_update_batchid = @idw_batchid')
--Insert into #SQLStatement values (8009,	'FROM	'  +  @ODS_TableSchema + ' ' +@ODS_TableName	)										-- clare.ODS_Filteredfma_licenceapplication a 
--Insert into #SQLStatement values (8010, 'LEFT OUTER JOIN ')
--Insert into #SQLStatement values (8011,  @DelTableFull +' '+ @DelTable)														--clare.zzStage_Filteredfma_licenceapplication b
--Insert into #SQLStatement values (8012,	'on  ' + @ODS_TableName+'.'+ @PRIMARY_KEY + ' = ' + @DelTable+'.'+@PRIMARY_KEY)		--  LicenceApplication_BK = b.LicenceApplication_BK
--Insert into #SQLStatement values (8013,	'where  '  + @DelTable+'.'+@PRIMARY_KEY + ' is null' )		--  LicenceApplication_BK = b.LicenceApplication_BK


DECLARE @DEL_PK_Name varchar(200)
Set @DEL_PK_Name = 'ColumnName'

Insert into #SQLStatement values (8030, '                    ')	
Insert into #SQLStatement values (8031, '                    ')	
Insert into #SQLStatement values (8032, '                    ')	
Insert into #SQLStatement values (8033, '--Update delete recprds to inactive   ')	
Insert into #SQLStatement values (8032, '                    ')	
Insert into #SQLStatement values (8032, 'if (Select count(*) from clare.fma_softdelete_tracking where TableName = ''' +@i_tableName+ ''')>0' )	
Insert into #SQLStatement values (8032, 'Begin                    ')	
Insert into #SQLStatement values (8032, '                    ')	
Insert into #SQLStatement values (8034, 'UPDATE ' + @ODS_TableSchema		)															--  clare.ODS_Filteredfma_licenceapplication
Insert into #SQLStatement values (8035,  'SET idw_version_end_date = getdate()')
Insert into #SQLStatement values (8036,  ',row_change_type = ''D''')
Insert into #SQLStatement values (8037,  ',idw_update_batchid = @idw_batchid')
Insert into #SQLStatement values (8038,	'FROM	'  +  @ODS_TableSchema + ' ' +@ODS_TableName	)										-- clare.ODS_Filteredfma_licenceapplication a 
Insert into #SQLStatement values (8039, 'LEFT OUTER JOIN ')
Insert into #SQLStatement values (8041,  @Del_tablename +' '+ @DelTable)														--clare.zzStage_Filteredfma_licenceapplication b
Insert into #SQLStatement values (8042,	'on  ' + @ODS_TableName+'.'+ @PRIMARY_KEY + ' = ' +@DEL_PK_Name)		--  LicenceApplication_BK = b.LicenceApplication_BK
Insert into #SQLStatement values (8043,	'and TableName = ''' + @i_tableName+ '''')		--  LicenceApplication_BK = b.LicenceApplication_BK
Insert into #SQLStatement values (8044,	'where  '  + @DEL_PK_Name + ' is null' )		--  LicenceApplication_BK = b.LicenceApplication_BK
Insert into #SQLStatement values (8045,	'          ' )		
Insert into #SQLStatement values (8046,	'end' )		

Insert into #SQLStatement values (8100, '     '  )
Insert into #SQLStatement values (8101, '     ' )
Insert into #SQLStatement values (8102, ' /* -----------------------------------    ' )
Insert into #SQLStatement values (8103, '  -- CREATE CURRENT VIEW   ' )
Insert into #SQLStatement values (8104, ' Create view  v_'+ @ODS_TableName + ' as'  )
Insert into #SQLStatement values (8105, ' Select * from '+  @ODS_TableSchema  )
Insert into #SQLStatement values (8106, ' Where IDW_Current = 1' )
Insert into #SQLStatement values (8107, ' */ -----------------------------------    ' )




			
end








/*

This section includes the auditing scripts
 */

Insert into #SQLStatement values (2000,' DECLARE @SPID INT, @JobNumber VARCHAR(50), @JobName VARCHAR(50), @DatabaseName VARCHAR(50), @SchemaName VARCHAR(50), @TableName VARCHAR(50), @ErrorMessage VARCHAR(200), @ErrorCode VARCHAR(10), @Job_Status VARCHAR(20), @Process_Step VARCHAR(20);')
Insert into #SQLStatement values (2001,'     SET @JobNumber = (')
Insert into #SQLStatement values (2002,'     ')
Insert into #SQLStatement values (2003,'         SELECT NEWID()')
Insert into #SQLStatement values (2004,'     )')
Insert into #SQLStatement values (2005,' SET @JobName		= (Select distinct object_name(@@PROCID));')
Insert into #SQLStatement values (2006,'     SET @DatabaseName=  (select db_name());')
Insert into #SQLStatement values (2007,'     SET @SchemaName= '''+@i_schemaName+''';')
Insert into #SQLStatement values (2008,'     SET @TableName= '''+@ODS_TableName +''';')
Insert into #SQLStatement values (2009,'     SET @Job_Status= ''Running'';')
Insert into #SQLStatement values (2010,'     SET @Process_Step= ''load/stage/fact/dimension'';')
Insert into #SQLStatement values (2011,'')
Insert into #SQLStatement values (2012,'/*')
Insert into #SQLStatement values (2013,'START LOGGING ALL AUDIT TABLE INFORMATION')
Insert into #SQLStatement values (2014,'')
Insert into #SQLStatement values (2015,'*/')
Insert into #SQLStatement values (2016,'                                        ')
Insert into #SQLStatement values (2017,'     EXEC dbo.proc_AuditProcedures ')
Insert into #SQLStatement values (2018,'          @JobNumber, ')
Insert into #SQLStatement values (2019,'          @JobName, ')
Insert into #SQLStatement values (2020,'          @DatabaseName, ')
Insert into #SQLStatement values (2021,'          @SchemaName, ')
Insert into #SQLStatement values (2022,'          @TableName, ')
Insert into #SQLStatement values (2023,'          @Job_Status, ')
Insert into #SQLStatement values (2024,'          @Process_Step;')
Insert into #SQLStatement values (2025,'')
Insert into #SQLStatement values (2026,'     -- ENTER MAIN CODE HERE')
Insert into #SQLStatement values (2027,'')
Insert into #SQLStatement values (2028,'     SET NOCOUNT ON;')
Insert into #SQLStatement values (2029,'    BEGIN TRY')
Insert into #SQLStatement values (2030,'')
Insert into #SQLStatement values (2031,'/* -------------------------------------------------------------------------')
Insert into #SQLStatement values (2032,'ENTER ALL CODE IN THE SECTION BELOW')
Insert into #SQLStatement values (2033,'*/')
Insert into #SQLStatement values (2034,'')
Insert into #SQLStatement values (2035,'        --------------------------------------------------------------------------')


Insert into #SQLStatement values (9000,'/*----------------------------------------------------------------------------')	
Insert into #SQLStatement values (9001,'NO MORE CUSTOMER CODE BELOW')	
Insert into #SQLStatement values (9002,'*/')	
Insert into #SQLStatement values (9003,'')	
Insert into #SQLStatement values (9004,'        ----------------------------------------------------------------------------')	
Insert into #SQLStatement values (9005,'')	
Insert into #SQLStatement values (9006,'    END TRY')	
Insert into #SQLStatement values (9007,'    BEGIN CATCH')	
Insert into #SQLStatement values (9008,'        SET @ErrorMessage = (ERROR_MESSAGE());')	
Insert into #SQLStatement values (9009,'        SET @ErrorCode = (ERROR_NUMBER());')	
Insert into #SQLStatement values (9010,'    END CATCH;')	
Insert into #SQLStatement values (9011,'     SET NOCOUNT OFF;')	
Insert into #SQLStatement values (9012,'')	
Insert into #SQLStatement values (9013,'/*')	
Insert into #SQLStatement values (9014,'END LOGGING')	
Insert into #SQLStatement values (9015,'*/')	
Insert into #SQLStatement values (9016,'')	
Insert into #SQLStatement values (9017,'     IF LEN(@ErrorCode) > 1')	
Insert into #SQLStatement values (9018,'         BEGIN')	
Insert into #SQLStatement values (9019,'             EXEC dbo.proc_AuditProcedures_Update')	
Insert into #SQLStatement values (9020,'                  @JobNumber, ')	
Insert into #SQLStatement values (9021,'                  @JobName, ')	
Insert into #SQLStatement values (9022,'                  @ErrorMessage, ')	
Insert into #SQLStatement values (9023,'                  ''ERROR'';')	
Insert into #SQLStatement values (9024,'     END;')	
Insert into #SQLStatement values (9025,'         ELSE')	
Insert into #SQLStatement values (9026,'         BEGIN')	
Insert into #SQLStatement values (9027,'             EXEC dbo.proc_AuditProcedures_Update')	
Insert into #SQLStatement values (9028,'                  @JobNumber, ')	
Insert into #SQLStatement values (9029,'                  @JobName, ')	
Insert into #SQLStatement values (9030,'                  @ErrorMessage, ')	
Insert into #SQLStatement values (9031,'                  ''Completed'';')	
Insert into #SQLStatement values (9032,'     END;')	

Select * from #SQLStatement
Order by OrderNum




--drop table clare.zz_stage_Filteredfma_licenceapplication

--Select * from 

GO


