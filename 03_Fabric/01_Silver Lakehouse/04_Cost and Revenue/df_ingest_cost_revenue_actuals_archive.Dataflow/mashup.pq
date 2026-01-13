[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_cost_revenue_actuals_archive_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_cost_revenue_actuals_archive = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP BW/Costs and Revenue Actuals/")),
  #"Added custom 1" = Table.AddColumn(#"Filtered rows", "Year", each Text.Middle([Name], 0, 4)),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Current Year", each Date.Year(DateTime.LocalNow())),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Year", type text}, {"Current Year", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 1", each [Current Year] > [Year]),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 1", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34", "Column35"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32", "Column33", "Column34", "Column35"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data", "File Name", each if [Column2] = "Company Code" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column2] = "Company Code" then "Refresh Date" else [Date modified]),
    #"Added custom 5" = Table.AddColumn(#"Added Custom4", "Year", each if [Column2] = "Company Code" then "Year" else Text.Middle([File Name], 0, 4)),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 5", {"Content", "Name", "Date modified", "Column1"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Removed top rows" = Table.Skip(#"Promoted Headers", 2),
  #"Filtered rows 2" = Table.SelectRows(#"Removed top rows", each [Company Code] <> "Company Code" and [Company Code] <> ""),
  #"Added custom" = Table.AddColumn(#"Filtered rows 2", "Row Type", each "Actuals"),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Posting period", "Posting Period"}, {"Fiscal year", "Fiscal Year"}, {"Partner company code", "Partner Company Code"}, {"Partner object", "Partner Object"}, {"Partner object Name", "Partner Object Name"}, {"Cost object", "Cost Center"}, {"Cost object Name", "Cost Center Name"}, {"Purchasing document", "Purchasing Document"}, {"Profit center Name", "Profit Center Name"}, {"Profit center", "Profit Center"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Company Code", type text}, {"CO Document Number", type text}, {"CO Doc Line Item Txt", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"Cost Element", type text}, {"Cost Element Name", type text}, {"Purchasing Document", type text}, {"PO Vendor", type text}, {"Material", type text}, {"Material Name", type text}, {"PO Item Text", type text}, {"Posting Period", type text}, {"Fiscal Year", type text}, {"Company ID", type text}, {"Doc.number", type text}, {"Customer Cost Center", type text}, {"Offsetting Account", type text}, {"Offsetting Account Name", type text}, {"Origin Object", type text}, {"Origin Object Name", type text}, {"Partner Company Code", type text}, {"Partner Object", type text}, {"Partner Object Name", type text}, {"Payer", type text}, {"Payer Name", type text}, {"Reversal ref. number", type text}, {"WBS Element", type text}, {"User Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Currency", type text}, {"Amount", type text}, {"Quantity", type text}, {"File Name", type text}, {"Refresh Date", type text}, {"Year", type text}, {"Row Type", type text}})
in
    #"Changed column type";
shared fact_cost_revenue_actuals_archive_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_cost_revenue_actuals_archive", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
