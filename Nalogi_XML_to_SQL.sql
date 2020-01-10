-- НАЧАЛО ПРОГРАММЫ

-- ИСХОДНЫЕ ДАННЫЕ
Declare @n_max_xml as int, @n_min_xml as int, @i as int;
Declare @Path as varchar(256), @param as varchar(256);
Declare @table_name as varchar(30);
Declare @Is_INNUL_Index bit;

-------------------------------------------------------------------------- ИСХОДНЫЕ ДАННЫЕ ----------------------------------------------------------------------

use taxpaid -- Выбрать БД
SET @table_name = 'taxpaid2018'; -- Название новой таблицы с данными из xml-ей
set @n_min_xml = 1100; --Мин. значение в имени файла
set @n_max_xml = 12150; --Макс. значение в имени файла
SET @path = 'C:\Users\beregovoydv\Downloads\0\'; -- Директория с файлами xml
SET @Is_INNUL_Index = 1; -- Сделать ИННЮЛ индексированным, некластеризованным? Да - 1, нет - 0.

--DROP TABLE dbo.[taxpaid2018]; DROP TABLE dbo.[XMLwithOpenXMLFile];
-----------------------------------------------------------------------------------------------------------------------------------------------------------------




-- ОСНОВНАЯ ЧАСТЬ

-- ЧАСТЬ 0. Создать новую таблицу на этот год
Declare @PreparedBatch as Nvarchar(max), @Batch as Varchar(max);

SET @Batch = 
'CREATE TABLE [dbo].[' + @table_name + '] 
(
	[УникНомер] [bigint] IDENTITY(1,1) NOT NULL,
	[ДатаДок] [varchar](15) NULL,
	[ДатаСост] [varchar](15) NULL,
	[НаимОрг] [varchar](1024) NULL,
	[ИННЮЛ] bigint NULL,
	[НаимНалог] [varchar](1024) NULL,
	[СумУплНал] money NULL,
	[Файл] [varchar](256) NULL,
	CONSTRAINT [PK_' + @table_name + '] PRIMARY KEY CLUSTERED 
	( [УникНомер] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];';

BEGIN TRY
	Exec (@Batch);
END TRY
BEGIN CATCH
	declare @str as varchar(max);
	set @str = 'Не получается создать таблицу. Возможно таблица ' + @table_name + ' уже существует. Удалите ее или укажите другое название таблицы!
	Для удаления выполните команду: DROP TABLE dbo.[' + @table_name + ']' ;
	RAISERROR (@str, 16, 1)
	RETURN 
END CATCH

-- ЧАСТЬ 1. Создать временно таблицу с xml-файлами
BEGIN TRY
	Create table XMLwithOpenXMLFile ( XmlData xml );
END TRY
BEGIN CATCH
	set @str = 'Не получается создать таблицу. Возможно таблица XMLwithOpenXMLFile уже существует. Удалите ее или укажите другое название таблицы!
	Для удаления выполните команду: DROP TABLE dbo.[XMLwithOpenXMLFile]' ;
	RAISERROR (@str, 16, 1)
	RETURN 
END CATCH


-- ЧАСТЬ 2. Заполнить таблицу с данными из таблицы с xml-файлами
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
	-- Текущее расположение xml-файла
	SET @param = @Path + '0 ('+ cast(@i as varchar(10)) + ').xml';
	
	BEGIN TRY
		BEGIN TRAN
			-- Создаем и выполняем Батч с этим файлом
			Select @Batch = Replace(@PreparedBatch,'?',@param)    
			Exec (@Batch);
    
			-- Записываем xml-файл во временную таблицу
			SET @Batch = '
			DECLARE @XML AS XML, @hDoc AS INT;
			SELECT @XML = XMLData FROM XMLwithOpenXMLFile
			EXEC sp_xml_preparedocument @hDoc OUTPUT, @XML
	

			-- Добавляем необходимые столбцы в основную таблицу
			INSERT INTO [' + @table_name + ']
			( [ДатаДок],[ДатаСост],[НаимОрг],[ИННЮЛ],[НаимНалог],[СумУплНал],[Файл] )
			SELECT [ДатаДок], [ДатаСост], [НаимОрг], [ИННЮЛ], [НаимНалог], [СумУплНал], ''' + @param + '''
			FROM OPENXML(@hDoc, ''Файл/Документ/СвУплСумНал'')
			WITH
			(
			     [ДатаДок] [varchar](1000) ''../@ДатаДок''
				,[ДатаСост] [varchar](1000) ''../@ДатаСост''
				,[НаимОрг] [varchar](1000) ''../СведНП/@НаимОрг''
				,[ИННЮЛ] [varchar](1000) ''../СведНП/@ИННЮЛ''
				,[НаимНалог] [varchar](1000) ''@НаимНалог''
				,[СумУплНал] [varchar](1000) ''@СумУплНал''
			)

			EXEC sp_xml_removedocument @hDoc';
		
			Exec (@Batch);	
			
		COMMIT TRAN
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN
		SET @errors += @param + '; ';
	END CATCH
	
	-- Очищает временную таблицу для следующего файла
	TRUNCATE TABLE XMLwithOpenXMLFile;

	-- Переходим к следующему файлу
	SET @i += 1;
	
	IF ((@i - @n_min_xml) % 500) = 0
		SELECT @i - @n_min_xml AS [XML-Файлов импортировано]
END

-- ЧАСТЬ 3. Удаляем таблицу для временного пользования
drop table XMLwithOpenXMLFile

-- ЧАСТЬ 4. Выводим список НЕзагруженных файлов
IF @errors = '' 
	SET @errors = 'НЕТ ошибок при импорте данных! Все файлы успешно добавлены в БД'
ELSE
	SET @errors = 'Скрипт выполнен с ошибками. Ошибка при загрузки файлов (добавлены не были):' + @errors;

SELECT @errors AS [ОШИБКИ!]

-- ЧАСТЬ 5. Делаем колонку ИННЮЛ индексированным, некластеризованным
SET @Batch = 'CREATE INDEX [ИННЮЛIndex_' + @table_name + '] ON [' + @table_name +'] ([ИННЮЛ]);';
Exec (@Batch);

-- ЧАСТЬ 6. Выводим первые 20 строчек данных новой таблицы для визуального осмотра
SET @Batch = 'SELECT top(20) * FROM [' + @table_name + ']';
Exec (@Batch);	


