USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_TARGET_SF_Accounts]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_TARGET_SF_Accounts]
GO

CREATE TABLE [dbo].[STG_TARGET_SF_Accounts](
  [Phone] [nvarchar](12) NOT NULL,
  [Name] [nvarchar](100) NOT NULL,
  [Industry] [nvarchar](50) NOT NULL,
  [AnnualRevenue] DECIMAL(16,2) NOT NULL,
  [Account_Number_External_ID__c] [nvarchar](36) PRIMARY KEY,
  [NumberofLocations__c] INT NOT NULL,
  [NumberOfEmployees] INT NOT NULL,
  [SLA__c] [nvarchar](20) NOT NULL,
  [SLASerialNumber__c] INT NOT NULL
) ON [PRIMARY]
GO
