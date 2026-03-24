USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_TARGET_SF_Contacts]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_TARGET_SF_Contacts]
GO

CREATE TABLE [dbo].[STG_TARGET_SF_Contacts](
  [account_number_external_id] [nvarchar](36),
  [first_name] [nvarchar](50) NOT NULL,
  [last_name] [nvarchar](50) NOT NULL,
  [AccountId] [nvarchar](50) NOT NULL,
  [Name] [nvarchar](100) NOT NULL,
  [Email] [nvarchar](50) NOT NULL,
  [Title] [nvarchar](50) NOT NULL,
  [Department] [nvarchar](50) NOT NULL,
  [Languages__c] [nvarchar](50) NOT NULL,
  [Phone] [nvarchar](50) NOT NULL,
  PRIMARY KEY (first_name, last_name)
) ON [PRIMARY]
GO
