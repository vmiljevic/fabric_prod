[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "map_rate_card_ce_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared map_rate_card_ce = let
    Source = SharePoint.Files("https://allianzms.sharepoint.com/teams/AT0090-10144432/", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "HR/Rate Cards") ),
    #"Removed Other Columns4" = Table.SelectColumns(#"Filtered rows", {"Content", "Name", "Date modified"}),
    #"Added Custom1" = Table.AddColumn(#"Removed Other Columns4", "Extracted Data", each Table.SelectRows(Excel.Workbook([Content]), each [Item] ="Sheet1")),
    #"Expanded Extracted Data2" = Table.ExpandTableColumn(#"Added Custom1", "Extracted Data", {"Data"}, {"Data"}),
  #"Expanded Data 1" = Table.ExpandTableColumn(#"Expanded Extracted Data2", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5"}, {"Column1", "Column2", "Column3", "Column4", "Column5"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded Data 1", "File Name", each if [Column1] = "Company Code" then "File Name" else [Name]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom2", "Refresh Date", each if [Column1] = "Company Code" then "Refresh Date" else [Date modified]),
  #"Removed columns" = Table.RemoveColumns(#"Added Custom4", {"Content", "Name", "Date modified"}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Removed columns", [PromoteAllScalars=true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted Headers", {{"Personal #(lf)number", "HR ID"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns", each ([Company Code] <> "Company Code")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 1", {{"Company Code", type text}, {"HR ID", type text}, {"Full name", type text}, {"Rate Card", type text}, {"Year", type text}, {"File Name", type text}, {"Refresh Date", type text}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Rate Card Key", each [Year] & "-" & [HR ID]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Rate Card Key", type text}})
in
    #"Changed column type 1";
shared map_rate_card_ce_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "map_rate_card_ce", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
