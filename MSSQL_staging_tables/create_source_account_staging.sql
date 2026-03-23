USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[STG_SOURCE_Accounts]  Script Date: 3/23/2026 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[STG_SOURCE_Accounts]
GO

CREATE TABLE [dbo].[STG_SOURCE_Accounts](
  [phone] [nvarchar](12) NOT NULL,
  [company_name] [nvarchar](100) NOT NULL,
  [industry] [nvarchar](50) NOT NULL,
  [annual_revenue] DECIMAL(16,2) NOT NULL,
  [account_number_external_id] [nvarchar](36) PRIMARY KEY,
  [number_of_locations] INT NOT NULL,
  [number_of_employees] INT NOT NULL,
  [sla] [nvarchar](20) NOT NULL,
  [sla_serial_number] INT NOT NULL
) ON [PRIMARY]
GO
