USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_SOURCE_Contracts]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_SOURCE_Contracts]
GO

CREATE TABLE [dbo].[STG_SOURCE_Contracts](
  [account_number_external_id] [nvarchar](36) NOT NULL,
  [contract_number] [nvarchar](36) PRIMARY KEY,
  [start_date] DATE NOT NULL,
  [end_date] DATE NOT NULL
) ON [PRIMARY]
GO
