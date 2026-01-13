[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_vendor_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_vendor = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dim_vendor", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"Refresh Date", type datetime}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"CoCd", "Company Code"}, {"Vendor", "Vendor Number"}, {"Name 1", "Vendor Name"}, {"Cty", "Country"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"PO Box pcd"}),
  #"Added custom" = Table.AddColumn(#"Removed columns", "Vendor Number and Name", each [Vendor Number] & " - " & [Vendor Name]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Vendor Key", each [Company Code] & "-" & [Vendor Number]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Vendor Number and Name", type text}, {"Vendor Key", type text}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type 1", {"Pers.No_"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"VAT Registration No_", "VAT"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"PayT", "DelF"})
in
  #"Removed columns 2";
shared dim_vendor_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_vendor", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
