[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_report_config_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_report_config = let
    Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-10144432/Shared%20Documents/01_Power%20BI/01_Data/Admin/FabricConfig.xlsx"), null, true),
  Navigation = Source{[Item = "PROD", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Config Key", type text}, {"Config Value", type text}})
in
    #"Changed column type";
shared dim_report_config_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a1aaa939-0dd1-4de5-804b-126e55a75f88"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_report_config", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
