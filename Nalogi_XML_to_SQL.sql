-- ������ ���������

-- �������� ������
Declare @n_max_xml as int, @n_min_xml as int, @i as int;
Declare @Path as varchar(256), @param as varchar(256);
Declare @table_name as varchar(30);
Declare @Is_INNUL_Index bit;

-------------------------------------------------------------------------- �������� ������ ----------------------------------------------------------------------

use taxpaid -- ������� ��
SET @table_name = 'taxpaid2018'; -- �������� ����� ������� � ������� �� xml-��
set @n_min_xml = 1100; --���. �������� � ����� �����
set @n_max_xml = 12150; --����. �������� � ����� �����
SET @path = 'C:\Users\beregovoydv\Downloads\0\'; -- ���������� � ������� xml
SET @Is_INNUL_Index = 1; -- ������� ����� ���������������, ������������������? �� - 1, ��� - 0.

--DROP TABLE dbo.[taxpaid2018]; DROP TABLE dbo.[XMLwithOpenXMLFile];
-----------------------------------------------------------------------------------------------------------------------------------------------------------------




-- �������� �����

-- ����� 0. ������� ����� ������� �� ���� ���
Declare @PreparedBatch as Nvarchar(max), @Batch as Varchar(max);

SET @Batch = 
'CREATE TABLE [dbo].[' + @table_name + '] 
(
	[���������] [bigint] IDENTITY(1,1) NOT NULL,
	[�������] [varchar](15) NULL,
	[��������] [varchar](15) NULL,
	[�������] [varchar](1024) NULL,
	[�����] bigint NULL,
	[���������] [varchar](1024) NULL,
	[���������] money NULL,
	[����] [varchar](256) NULL,
	CONSTRAINT [PK_' + @table_name + '] PRIMARY KEY CLUSTERED 
	( [���������] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];';

BEGIN TRY
	Exec (@Batch);
END TRY
BEGIN CATCH
	declare @str as varchar(max);
	set @str = '�� ���������� ������� �������. �������� ������� ' + @table_name + ' ��� ����������. ������� �� ��� ������� ������ �������� �������!
	��� �������� ��������� �������: DROP TABLE dbo.[' + @table_name + ']' ;
	RAISERROR (@str, 16, 1)
	RETURN 
END CATCH

-- ����� 1. ������� �������� ������� � xml-�������
BEGIN TRY
	Create table XMLwithOpenXMLFile ( XmlData xml );
END TRY
BEGIN CATCH
	set @str = '�� ���������� ������� �������. �������� ������� XMLwithOpenXMLFile ��� ����������. ������� �� ��� ������� ������ �������� �������!
	��� �������� ��������� �������: DROP TABLE dbo.[XMLwithOpenXMLFile]' ;
	RAISERROR (@str, 16, 1)
	RETURN 
END CATCH


-- ����� 2. ��������� ������� � ������� �� ������� � xml-�������
set @n_max_xml += 1;
set @i = @n_min_xml;

Set @PreparedBatch =
'DECLARE @XML XML
			
SELECT @xml = CAST(BulkColumn AS XML)
FROM OPENROWSET( BULK ''?'', SINGLE_BLOB) as x
			
INSERT INTO XMLwithOpenXMLFile ( XmlData )
SELECT @xml;';

DECLARE @errors AS VARCHAR(MAX);
SET @errors = '';

WHILE @i < @n_max_xml
BEGIN
	-- ������� ������������ xml-�����
	SET @param = @Path + '0 ('+ cast(@i as varchar(10)) + ').xml';
	
	BEGIN TRY
		BEGIN TRAN
			-- ������� � ��������� ���� � ���� ������
			Select @Batch = Replace(@PreparedBatch,'?',@param)    
			Exec (@Batch);
    
			-- ���������� xml-���� �� ��������� �������
			SET @Batch = '
			DECLARE @XML AS XML, @hDoc AS INT;
			SELECT @XML = XMLData FROM XMLwithOpenXMLFile
			EXEC sp_xml_preparedocument @hDoc OUTPUT, @XML
	

			-- ��������� ����������� ������� � �������� �������
			INSERT INTO [' + @table_name + ']
			( [�������],[��������],[�������],[�����],[���������],[���������],[����] )
			SELECT [�������], [��������], [�������], [�����], [���������], [���������], ''' + @param + '''
			FROM OPENXML(@hDoc, ''����/��������/�����������'')
			WITH
			(
			     [�������] [varchar](1000) ''../@�������''
				,[��������] [varchar](1000) ''../@��������''
				,[�������] [varchar](1000) ''../������/@�������''
				,[�����] [varchar](1000) ''../������/@�����''
				,[���������] [varchar](1000) ''@���������''
				,[���������] [varchar](1000) ''@���������''
			)

			EXEC sp_xml_removedocument @hDoc';
		
			Exec (@Batch);	
			
		COMMIT TRAN
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN
		SET @errors += @param + '; ';
	END CATCH
	
	-- ������� ��������� ������� ��� ���������� �����
	TRUNCATE TABLE XMLwithOpenXMLFile;

	-- ��������� � ���������� �����
	SET @i += 1;
	
	IF ((@i - @n_min_xml) % 500) = 0
		SELECT @i - @n_min_xml AS [XML-������ �������������]
END

-- ����� 3. ������� ������� ��� ���������� �����������
drop table XMLwithOpenXMLFile

-- ����� 4. ������� ������ ������������� ������
IF @errors = '' 
	SET @errors = '��� ������ ��� ������� ������! ��� ����� ������� ��������� � ��'
ELSE
	SET @errors = '������ �������� � ��������. ������ ��� �������� ������ (��������� �� ����):' + @errors;

SELECT @errors AS [������!]

-- ����� 5. ������ ������� ����� ���������������, ������������������
SET @Batch = 'CREATE INDEX [�����Index_' + @table_name + '] ON [' + @table_name +'] ([�����]);';
Exec (@Batch);

-- ����� 6. ������� ������ 20 ������� ������ ����� ������� ��� ����������� �������
SET @Batch = 'SELECT top(20) * FROM [' + @table_name + ']';
Exec (@Batch);	


