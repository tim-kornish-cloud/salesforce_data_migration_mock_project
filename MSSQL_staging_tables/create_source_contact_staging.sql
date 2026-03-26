USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_SOURCE_Contacts]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_SOURCE_Contacts]
GO

CREATE TABLE [dbo].[STG_SOURCE_Contacts](
  [account_number_external_id] [nvarchar](36),
  [first_name] [nvarchar](200) NOT NULL,
  [last_name] [nvarchar](200) NOT NULL,
  [email] [nvarchar](200) NOT NULL,
  [title] [nvarchar](200) NOT NULL,
  [department] [nvarchar](200) NOT NULL,
  [languages] [nvarchar](200) NOT NULL,
  PRIMARY KEY (first_name, last_name)

) ON [PRIMARY]
GO
