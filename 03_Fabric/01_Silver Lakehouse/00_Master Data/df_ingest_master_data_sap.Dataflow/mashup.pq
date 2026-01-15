[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cost_center_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Cost Center", DestinationColumnName = "Cost Center"], [SourceColumnName = "Cost Center Name", DestinationColumnName = "Cost Center Name"], [SourceColumnName = "Company Code", DestinationColumnName = "Company Code"], [SourceColumnName = "Company Code Name", DestinationColumnName = "Company Code Name"], [SourceColumnName = "Profit Center", DestinationColumnName = "Profit Center"], [SourceColumnName = "Profit Center Name", DestinationColumnName = "Profit Center Name"], [SourceColumnName = "Pillar", DestinationColumnName = "Pillar"], [SourceColumnName = "Person Responsible", DestinationColumnName = "Person Responsible"], [SourceColumnName = "Department", DestinationColumnName = "Department"], [SourceColumnName = "Costcenter Typ 1", DestinationColumnName = "Costcenter Typ 1"], [SourceColumnName = "Costcenter Typ 2", DestinationColumnName = "Costcenter Typ 2"], [SourceColumnName = "Costcenter Typ 3", DestinationColumnName = "Costcenter Typ 3"], [SourceColumnName = "Lock Indicator  Actuals Primary Cost", DestinationColumnName = "Lock Indicator  Actuals Primary Cost"], [SourceColumnName = "Lock Indicator Actuals Secondary Cost", DestinationColumnName = "Lock Indicator Actuals Secondary Cost"], [SourceColumnName = "Valid From", DestinationColumnName = "Valid From"], [SourceColumnName = "Valid To", DestinationColumnName = "Valid To"], [SourceColumnName = "File Name", DestinationColumnName = "File Name"], [SourceColumnName = "Refresh Date", DestinationColumnName = "Refresh Date"], [SourceColumnName = "Year", DestinationColumnName = "Year"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_cost_center_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_CC.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Cost Center" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Cost Center" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Cost Center", type text}, {"Cost Center Name", type text}, {"Company Code", type text}, {"Company Code Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Pillar", type text}, {"Person Responsible", type text}, {"Department", type text}, {"Costcenter Typ 1", type text}, {"Costcenter Typ 2", type text}, {"Costcenter Typ 3", type text}, {"Lock Indicator  Actuals Primary Cost", type text}, {"Lock Indicator Actuals Secondary Cost", type text}, {"Valid From", type text}, {"Valid To", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cost_center_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Cost Center", DestinationColumnName = "Cost Center"], [SourceColumnName = "Cost Center Name", DestinationColumnName = "Cost Center Name"], [SourceColumnName = "Company Code", DestinationColumnName = "Company Code"], [SourceColumnName = "Company Code Name", DestinationColumnName = "Company Code Name"], [SourceColumnName = "Profit Center", DestinationColumnName = "Profit Center"], [SourceColumnName = "Profit Center Name", DestinationColumnName = "Profit Center Name"], [SourceColumnName = "Pillar", DestinationColumnName = "Pillar"], [SourceColumnName = "Person Responsible", DestinationColumnName = "Person Responsible"], [SourceColumnName = "Department", DestinationColumnName = "Department"], [SourceColumnName = "Costcenter Typ 1", DestinationColumnName = "Costcenter Typ 1"], [SourceColumnName = "Costcenter Typ 2", DestinationColumnName = "Costcenter Typ 2"], [SourceColumnName = "Costcenter Typ 3", DestinationColumnName = "Costcenter Typ 3"], [SourceColumnName = "Lock Indicator  Actuals Primary Cost", DestinationColumnName = "Lock Indicator  Actuals Primary Cost"], [SourceColumnName = "Lock Indicator Actuals Secondary Cost", DestinationColumnName = "Lock Indicator Actuals Secondary Cost"], [SourceColumnName = "Valid From", DestinationColumnName = "Valid From"], [SourceColumnName = "Valid To", DestinationColumnName = "Valid To"], [SourceColumnName = "File Name", DestinationColumnName = "File Name"], [SourceColumnName = "Refresh Date", DestinationColumnName = "Refresh Date"], [SourceColumnName = "Year", DestinationColumnName = "Year"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_cost_center_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_CC.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Cost Center" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Cost Center" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Cost Center", type text}, {"Cost Center Name", type text}, {"Company Code", type text}, {"Company Code Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Pillar", type text}, {"Person Responsible", type text}, {"Department", type text}, {"Costcenter Typ 1", type text}, {"Costcenter Typ 2", type text}, {"Costcenter Typ 3", type text}, {"Lock Indicator  Actuals Primary Cost", type text}, {"Lock Indicator Actuals Secondary Cost", type text}, {"Valid From", type text}, {"Valid To", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_payer_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_payer_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Payer.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Payer" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Payer" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear", "Chief Technology & Transformation Officers", "Chief Technology & Transformation Officers Name", "Column11"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"Groups & Regions", "Groups And Regions"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Payer", type text}, {"Payer Name", type text}, {"Country", type text}, {"Region", type text}, {"Account", type text}, {"Account Group", type text}, {"Groups And Regions", type text}, {"GL_Region", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_payer_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_payer_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Payer.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Payer" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Payer" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear", "Chief Technology & Transformation Officers", "Chief Technology & Transformation Officers Name", "Column11"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns 1", {{"Groups & Regions", "Groups And Regions"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Payer", type text}, {"Payer Name", type text}, {"Country", type text}, {"Region", type text}, {"Account", type text}, {"Account Group", type text}, {"Groups And Regions", type text}, {"GL_Region", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
shared dim_payer_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_payer_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_payer_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_payer_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_profit_center_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_profit_center_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_PC.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column1] = "Profit Center" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Profit Center" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Profit Center", type text}, {"Profit Center Name", type text}, {"Valid From", type text}, {"Valid To", type text}, {"Person Responsible", type text}, {"Lock Indicator", type text}, {"Pillar", type text}, {"Profit Center Category", type text}, {"Division", type text}, {"Department", type text}, {"Status", type text}, {"Investment Type", type text}, {"Portfolio", type text}, {"Program Cluster", type text}, {"Program Cluster Name", type text}, {"Program", type text}, {"Program Name", type text}, {"Service Landscape", type text}, {"Service Landscape Name", type text}, {"Service Class", type text}, {"Service Class Name", type text}, {"Service Package", type text}, {"Service Package Name", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_profit_center_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_profit_center_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_PC.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column1] = "Profit Center" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Profit Center" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Profit Center", type text}, {"Profit Center Name", type text}, {"Valid From", type text}, {"Valid To", type text}, {"Person Responsible", type text}, {"Lock Indicator", type text}, {"Pillar", type text}, {"Profit Center Category", type text}, {"Division", type text}, {"Department", type text}, {"Status", type text}, {"Investment Type", type text}, {"Portfolio", type text}, {"Program Cluster", type text}, {"Program Cluster Name", type text}, {"Program", type text}, {"Program Name", type text}, {"Service Landscape", type text}, {"Service Landscape Name", type text}, {"Service Class", type text}, {"Service Class Name", type text}, {"Service Package", type text}, {"Service Package Name", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_wbs_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_wbs_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_WBS.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "WBS Element" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "WBS Element" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"WBS Element", type text}, {"WBS Element Name", type text}, {"Change Run Indicator", type text}, {"Creation Date", type text}, {"Changed on", type text}, {"PSP System Status", type text}, {"Project Start Date", type text}, {"Project End Date", type text}, {"Company Code", type text}, {"Company Code Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Responsible Cost Centre", type text}, {"Responsible Cost Centre Name", type text}, {"Short ID", type text}, {"Main WBS Element", type text}, {"Project Leader", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_wbs_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_wbs_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_WBS.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "WBS Element" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "WBS Element" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"WBS Element", type text}, {"WBS Element Name", type text}, {"Change Run Indicator", type text}, {"Creation Date", type text}, {"Changed on", type text}, {"PSP System Status", type text}, {"Project Start Date", type text}, {"Project End Date", type text}, {"Company Code", type text}, {"Company Code Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Responsible Cost Centre", type text}, {"Responsible Cost Centre Name", type text}, {"Short ID", type text}, {"Main WBS Element", type text}, {"Project Leader", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_materials_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_materials_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Materials.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column1] = "Material" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Material" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Material", type text}, {"Material Name", type text}, {"Created on", type text}, {"Material Pricing Grp", type text}, {"Material Pricing Grp_1", type text}, {"Cost Center", type text}, {"Cost Center_2", type text}, {"Sales Organization", type text}, {"Sales Organization_3", type text}, {"Material Status SalesOrg", type text}, {"Del.Ind. SalesOrg", type text}, {"Profit center", type text}, {"Profit center_4", type text}, {"Material Type", type text}, {"Material Group", type text}, {"AZT Landscape", type text}, {"AZT Service Class", type text}, {"AZT Service Package", type text}, {"Material Status", type text}, {"Deletion Indicator", type text}, {"Price Method", type text}, {"Indicator Quantity", type text}, {"Material Status Plan", type text}, {"Generally not in Scope  KPI", type text}, {"Implemented", type text}, {"Item category group from material master", type text}, {"Service Lifecycle", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_materials_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_materials_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Materials.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column1] = "Material" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Material" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Material", type text}, {"Material Name", type text}, {"Created on", type text}, {"Material Pricing Grp", type text}, {"Material Pricing Grp_1", type text}, {"Cost Center", type text}, {"Cost Center_2", type text}, {"Sales Organization", type text}, {"Sales Organization_3", type text}, {"Material Status SalesOrg", type text}, {"Del.Ind. SalesOrg", type text}, {"Profit center", type text}, {"Profit center_4", type text}, {"Material Type", type text}, {"Material Group", type text}, {"AZT Landscape", type text}, {"AZT Service Class", type text}, {"AZT Service Package", type text}, {"Material Status", type text}, {"Deletion Indicator", type text}, {"Price Method", type text}, {"Indicator Quantity", type text}, {"Material Status Plan", type text}, {"Generally not in Scope  KPI", type text}, {"Implemented", type text}, {"Item category group from material master", type text}, {"Service Lifecycle", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_material_prices_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_material_prices_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Material Prices.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Sales Organization" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Sales Organization" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] = [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Sales Organization", type text}, {"Sales Organization_1", type text}, {"Material", type text}, {"Material Name", type text}, {"Indicator Quantity", type text}, {"Price Group", type text}, {"Price Group_2", type text}, {"C_Valid From", type text}, {"C_Valid To", type text}, {"Profit center", type text}, {"Profit center_3", type text}, {"User Responsible", type text}, {"Condition Unit", type text}, {"Condition Unit Name", type text}, {"Unit of measure", type text}, {"Unit of Measure Name", type text}, {"Scale Currency", type text}, {"Scale Currency Name", type text}, {"Rate Unit", type text}, {"Rate Unit Name", type text}, {"Material Group", type text}, {"Currency", type text}, {"Currency Name", type text}, {"Amount", type text}, {"Conditonal Pricing Unit", type text}, {"Quantity", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_material_prices_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_material_prices_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared Documents/01_Power BI/01_Data/Master Data/SAP BW/") and ([Name] = "Master Data Reporting SAP_Material Prices.xlsx")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows",{"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column1] = "Sales Organization" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Sales Organization" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Year", each Date.Year([Refresh Date])),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Filtered rows 1" = Table.SelectRows(#"Added custom 1", each [#"CurrentYear"] > [Year]),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows 1", {"CurrentYear"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"Sales Organization", type text}, {"Sales Organization_1", type text}, {"Material", type text}, {"Material Name", type text}, {"Indicator Quantity", type text}, {"Price Group", type text}, {"Price Group_2", type text}, {"C_Valid From", type text}, {"C_Valid To", type text}, {"Profit center", type text}, {"Profit center_3", type text}, {"User Responsible", type text}, {"Condition Unit", type text}, {"Condition Unit Name", type text}, {"Unit of measure", type text}, {"Unit of Measure Name", type text}, {"Scale Currency", type text}, {"Scale Currency Name", type text}, {"Rate Unit", type text}, {"Rate Unit Name", type text}, {"Material Group", type text}, {"Currency", type text}, {"Currency Name", type text}, {"Amount", type text}, {"Conditonal Pricing Unit", type text}, {"Quantity", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_rates_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_rates_current = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each (Text.Contains([Folder Path],"/01_Power BI/01_Data/Master Data/SAP BW/")) and (Text.Contains([Name],"SAP_Rates.xlsx"))),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "Year", each try List.Reverse(Text.Split([Folder Path], "/")){1} otherwise null), {{"Year", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Added custom 2", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Added custom 1", {{"Year", Int64.Type}, {"CurrentYear", Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each [#"CurrentYear"] = [Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1",{"Content", "Name", "Date modified","Year"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column4] = "Sales Organization" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column4] = "Sales Organization" then "Refresh Date" else [Date modified]),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "Year New", each if [Column4] = "Sales Organization" then "Year" else [Year]),
  #"Removed columns 2" = Table.RemoveColumns(#"Added Custom5", {"Year"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed columns 2", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Changed column type 1", {{"H_Application", type text}, {"H_Usage Cond. Tab.", type text}, {"H_Condition Type", type text}, {"Sales Organization", type text}, {"Sales Organization Name", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"Material", type text}, {"Material Name", type text}, {"H_Condition Table", type text}, {"H_Valid From", type text}, {"H_Valid To", type text}, {"C_Valid From", type text}, {"C_Valid To", type text}, {"H_Sales condition No", type text}, {"P_Condt. Seq. No.", type text}, {"Condition Unit", type text}, {"Rate Description", type text}, {"Currency", type text}, {"Currency Name", type text}, {"Rate", type text}, {"Condit. pricing unit", type text}, {"Number of Records", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_rates_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_rates_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each (Text.Contains([Folder Path],"/01_Power BI/01_Data/Master Data/SAP BW/")) and (Text.Contains([Name],"SAP_Rates.xlsx"))),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Filtered rows", "Year", each try List.Reverse(Text.Split([Folder Path], "/")){1} otherwise null), {{"Year", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Added custom 2", "CurrentYear", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Added custom 1", {{"Year", Int64.Type}, {"CurrentYear", Int64.Type}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each [#"CurrentYear"] > [Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1",{"Content", "Name", "Date modified","Year"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column4] = "Sales Organization" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column4] = "Sales Organization" then "Refresh Date" else [Date modified]),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "Year New", each if [Column4] = "Sales Organization" then "Year" else [Year]),
  #"Removed columns 2" = Table.RemoveColumns(#"Added Custom5", {"Year"}),
  #"Removed columns" = Table.RemoveColumns(#"Removed columns 2", {"Content", "Date modified", "Name"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 1),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Refresh Date", type datetime}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Changed column type 1", {{"H_Application", type text}, {"H_Usage Cond. Tab.", type text}, {"H_Condition Type", type text}, {"Sales Organization", type text}, {"Sales Organization Name", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"Material", type text}, {"Material Name", type text}, {"H_Condition Table", type text}, {"H_Valid From", type text}, {"H_Valid To", type text}, {"C_Valid From", type text}, {"C_Valid To", type text}, {"H_Sales condition No", type text}, {"P_Condt. Seq. No.", type text}, {"Condition Unit", type text}, {"Rate Description", type text}, {"Currency", type text}, {"Currency Name", type text}, {"Rate", type text}, {"Condit. pricing unit", type text}, {"Number of Records", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}})
in
    #"Changed column type";
shared dim_profit_center_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_profit_center_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_cost_center_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_center_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_wbs_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_wbs_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_wbs_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_wbs_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_materials_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_materials_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_materials_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_materials_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_material_prices_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_material_prices_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_material_prices_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_material_prices_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_rates_current_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_rates_current", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_rates_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_rates_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_profit_center_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_profit_center_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_cost_center_archive_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_center_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
