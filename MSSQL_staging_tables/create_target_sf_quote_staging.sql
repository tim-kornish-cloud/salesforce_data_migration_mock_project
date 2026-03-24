USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_TARGET_SF_Quotes]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_TARGET_SF_Quotes]
GO

CREATE TABLE [dbo].[STG_TARGET_SF_Quotes](
  [account_number_external_id] [nvarchar](36) NOT NULL,
  [contract_external_id__c] [nvarchar](36) PRIMARY KEY,
  [SBQQ__Account__c] [nvarchar](36) NOT NULL,
  [SBQQ__PricebookId__c] [nvarchar](36) NOT NULL,
  [SBQQ__Opportunity2__c] [nvarchar](36) NOT NULL,
  [SBQQ__Primary__c] [nvarchar](10) NOT NULL,
  [SBQQ__Type__c] [nvarchar](36) NOT NULL,
  [SBQQ__Status__c] [nvarchar](36) NOT NULL,
  [SBQQ__StartDate__c] DATE NOT NULL,
  [SBQQ__EndDate__c] DATE NOT NULL
) ON [PRIMARY]
GO
