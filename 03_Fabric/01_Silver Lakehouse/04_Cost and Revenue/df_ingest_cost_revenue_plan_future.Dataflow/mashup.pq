[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_cost_revenue_plan_future_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_cost_revenue_plan_future = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/Costs and Revenue Plan/")),
  #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 0, 4)),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 1", each [Current Year] < [Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column2] = "Company Code" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column2] = "Company Code" then "Refresh Date" else [Date modified]),
    #"Added custom 5" = Table.AddColumn(#"Added Custom4", "Year", each if [Column2] = "Company Code" then "Year" else Text.Middle([File Name], 0, 4)),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 5", {"Content", "Name", "Date modified", "Column1"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 3),
  #"Filtered rows 2" = Table.SelectRows(#"Removed top rows", each [Company Code] <> "Company Code" and [Company Code] <> ""),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Filtered rows 2", {"Company Code", "Company Code Name", "Version", "Cost Element", "Cost Element Name", "Profit Center", "Profit Center Name", "Person responsible", "Cost Center", "Cost Center Name", "WBS Element", "WBS Element Name", "Material", "Material Name", "Debtor", "Debtor Name", "Customer Company Code", "Customer Object", "File Name", "Refresh Date", "Year"}, "Attribute", "Value"),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Unpivoted columns", {{"Year", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 2", {{"Attribute", "Posting Period"}, {"Value", "Amount"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Row Type", each "Plan"),
  #"Removed columns 1" = Table.RemoveColumns(#"Added custom", {"Year"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns 1", {"Version"}, m_plannning_versions, {"Version"}, "m_plannning_versions", JoinKind.LeftOuter),
  #"Expanded m_plannning_versions" = Table.ExpandTableColumn(#"Merged queries", "m_plannning_versions", {"Year"}, {"Year"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded m_plannning_versions", {{"Debtor", "Payer"}, {"Debtor Name", "Payer Name"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"Company Code", type text}, {"Company Code Name", type text}, {"Version", type text}, {"Cost Element", type text}, {"Cost Element Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Person responsible", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"WBS Element", type text}, {"WBS Element Name", type text}, {"Material", type text}, {"Material Name", type text}, {"Payer", type text}, {"Payer Name", type text}, {"Customer Company Code", type text}, {"Customer Object", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Posting Period", type text}, {"Amount", type text}, {"Row Type", type text}, {"Year", type text}})
in
    #"Changed column type";
shared m_plannning_versions = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Master%20Data/Local/Local%20Master%20Data.xlsx"), null, true),
    PlanningVersions_Table = Source{[Item="PlanningVersions",Kind="Table"]}[Data]
in
    PlanningVersions_Table;
shared fact_cost_revenue_plan_future_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_cost_revenue_plan_future", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
