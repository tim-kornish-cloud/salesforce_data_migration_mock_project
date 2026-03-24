USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_SOURCE_Contract_Lines]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_SOURCE_Contract_Lines]
GO

CREATE TABLE [dbo].[STG_SOURCE_Contract_Lines](
  [contract_line_number] [nvarchar](36) PRIMARY KEY,
  [contract_number] [nvarchar](36) NOT NULL,
  [product] [nvarchar](50) NOT NULL,
  [quantity] INT NOT NULL
) ON [PRIMARY]
GO
