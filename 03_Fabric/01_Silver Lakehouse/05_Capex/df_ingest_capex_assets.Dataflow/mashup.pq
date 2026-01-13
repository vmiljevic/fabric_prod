[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_capex_assets_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared dim_capex_assets = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "01_Power BI/01_Data/SAP CAP/Capex") ),
  #"Filtered rows 3" = Table.SelectRows(#"Filtered rows", each Text.Contains([Name], "Asset")),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows 3", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] = "Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}),
  #"Kept top rows" = Table.FirstN(#"Expanded Data", 50),
    #"Added Custom2" = Table.AddColumn(#"Kept top rows", "File Name", each if [Column12] = "Company Code" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column12] = "Company Code" then "Refresh Date" else [Date modified]),
      #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Name", "Date modified", "Column1"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted Headers", {{"Object", type text}, {"Cost Center", type text}, {"WBS Element for Acct Assignment of Costs", type text}, {"Asset Class", type text}, {"Capitaliz. date", type text}, {"Object type description", type text}, {"Description", type text}, {"Cuml. APC/repl.val.", type text}, {"Accumulated deprec.", type text}, {"Depr. 2025", type text}, {"Company Code", type text}, {"Depreciation key", type text}, {"Ord.dep.start date", type text}, {"Useful life", type text}, {"APC acquisition 2025", type text}, {"File Name", type text}, {"Refresh Date", type text}})
in
    #"Changed column type";
shared dim_capex_assets_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_capex_assets", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
